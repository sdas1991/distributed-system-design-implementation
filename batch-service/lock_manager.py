"""
Distributed Lock Manager for Batch Service

Implements leader election using Redis to ensure only one batch service instance
runs scheduled jobs at a time.
"""

import asyncio
import logging
import uuid
from typing import Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class DistributedLockManager:
    """Distributed lock manager using Redis"""

    def __init__(self, redis_url: str = "redis://redis:6379"):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None

        # Lua script for safe lock release
        self.release_lock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

        # Lua script for lock extension
        self.extend_lock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

    async def connect(self):
        """Connect to Redis"""
        try:
            self.client = redis.from_url(self.redis_url, decode_responses=True)
            await self.client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def acquire_lock(
        self,
        lock_key: str,
        ttl_ms: int = 30000  # 30 seconds default
    ) -> Optional[str]:
        """
        Acquire a distributed lock

        Args:
            lock_key: Unique identifier for the lock
            ttl_ms: Lock expiration time in milliseconds

        Returns:
            Lock token if acquired, None if failed
        """
        if not self.client:
            raise RuntimeError("Not connected to Redis")

        lock_token = str(uuid.uuid4())
        full_key = f"lock:{lock_key}"

        try:
            # SET with NX (only if not exists) and PX (expiration in ms)
            result = await self.client.set(
                full_key,
                lock_token,
                nx=True,
                px=ttl_ms
            )

            if result:
                logger.debug(f"Acquired lock: {lock_key} (token: {lock_token}, ttl: {ttl_ms}ms)")
                return lock_token
            else:
                logger.debug(f"Failed to acquire lock: {lock_key} (already held)")
                return None

        except Exception as e:
            logger.error(f"Error acquiring lock {lock_key}: {e}")
            return None

    async def release_lock(self, lock_key: str, lock_token: str) -> bool:
        """
        Release a distributed lock

        Args:
            lock_key: Lock identifier
            lock_token: Token returned from acquire_lock

        Returns:
            True if released successfully, False otherwise
        """
        if not self.client:
            raise RuntimeError("Not connected to Redis")

        full_key = f"lock:{lock_key}"

        try:
            result = await self.client.eval(
                self.release_lock_script,
                1,
                full_key,
                lock_token
            )

            if result == 1:
                logger.debug(f"Released lock: {lock_key} (token: {lock_token})")
                return True
            else:
                logger.warning(f"Failed to release lock: {lock_key} (token mismatch or expired)")
                return False

        except Exception as e:
            logger.error(f"Error releasing lock {lock_key}: {e}")
            return False

    async def extend_lock(
        self,
        lock_key: str,
        lock_token: str,
        ttl_ms: int = 30000
    ) -> bool:
        """
        Extend/refresh a lock's TTL

        Args:
            lock_key: Lock identifier
            lock_token: Token returned from acquire_lock
            ttl_ms: New TTL in milliseconds

        Returns:
            True if extended successfully, False otherwise
        """
        if not self.client:
            raise RuntimeError("Not connected to Redis")

        full_key = f"lock:{lock_key}"

        try:
            result = await self.client.eval(
                self.extend_lock_script,
                1,
                full_key,
                lock_token,
                str(ttl_ms)
            )

            if result == 1:
                logger.debug(f"Extended lock: {lock_key} (token: {lock_token}, ttl: {ttl_ms}ms)")
                return True
            else:
                logger.warning(f"Failed to extend lock: {lock_key} (token mismatch or expired)")
                return False

        except Exception as e:
            logger.error(f"Error extending lock {lock_key}: {e}")
            return False

    async def close(self):
        """Close Redis connection"""
        if self.client:
            await self.client.close()
            logger.info("Closed Redis connection")


class LeaderElection:
    """
    Leader Election using Distributed Locks

    Ensures only one instance of the batch service runs scheduled jobs.
    """

    def __init__(
        self,
        lock_manager: DistributedLockManager,
        service_name: str,
        instance_id: Optional[str] = None,
        lease_duration_ms: int = 30000  # 30 seconds
    ):
        self.lock_manager = lock_manager
        self.service_name = service_name
        self.instance_id = instance_id or str(uuid.uuid4())
        self.lease_duration_ms = lease_duration_ms
        self.leader_token: Optional[str] = None
        self.is_leader = False

    async def try_become_leader(self) -> bool:
        """
        Try to become the leader

        Returns:
            True if elected as leader, False otherwise
        """
        lock_key = f"leader:{self.service_name}"
        token = await self.lock_manager.acquire_lock(lock_key, self.lease_duration_ms)

        if token:
            self.leader_token = token
            self.is_leader = True
            logger.info(f"Instance {self.instance_id} became leader for {self.service_name}")
            return True

        self.is_leader = False
        return False

    async def renew_leadership(self) -> bool:
        """
        Renew leadership (extend the lock)

        Returns:
            True if renewed successfully, False if lost leadership
        """
        if not self.leader_token:
            return False

        lock_key = f"leader:{self.service_name}"
        renewed = await self.lock_manager.extend_lock(
            lock_key,
            self.leader_token,
            self.lease_duration_ms
        )

        if not renewed:
            logger.warning(f"Instance {self.instance_id} lost leadership for {self.service_name}")
            self.is_leader = False
            self.leader_token = None

        return renewed

    async def step_down(self):
        """Step down from leadership"""
        if not self.is_leader or not self.leader_token:
            return

        lock_key = f"leader:{self.service_name}"
        await self.lock_manager.release_lock(lock_key, self.leader_token)

        self.is_leader = False
        self.leader_token = None

        logger.info(f"Instance {self.instance_id} stepped down from leadership for {self.service_name}")

    def is_current_leader(self) -> bool:
        """Check if this instance is the current leader"""
        return self.is_leader
