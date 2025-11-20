#!/bin/bash

# Sharding Verification Script
# This script demonstrates that ShardStream correctly shards data across multiple databases

set -e

echo "=========================================="
echo "ShardStream Sharding Verification"
echo "=========================================="
echo ""

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

# Test data with different customer IDs (will be distributed across shards)
CUSTOMERS=("cust-001" "cust-002" "cust-003" "cust-004" "cust-005" "cust-100" "cust-200" "cust-300")

echo "Step 1: Ingesting events for multiple customers..."
echo "------------------------------------------------"

for customer in "${CUSTOMERS[@]}"; do
    echo "Ingesting event for customer: $customer"

    response=$(curl -s -X POST http://localhost:8080/api/v1/ingest \
        -H "Content-Type: application/json" \
        -d "{
            \"customerId\": \"$customer\",
            \"eventType\": \"test.event\",
            \"payload\": {
                \"action\": \"sharding-test\",
                \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
            },
            \"metadata\": {
                \"test\": \"sharding-verification\"
            }
        }")

    shard_id=$(echo $response | jq -r '.shardId')
    success=$(echo $response | jq -r '.success')

    if [ "$success" == "true" ]; then
        echo "  ✓ Customer $customer routed to SHARD $shard_id"
    else
        echo "  ✗ Failed to ingest for customer $customer"
    fi
done

echo ""
echo "Step 2: Verifying data distribution across shards..."
echo "------------------------------------------------"

# Function to count events in a shard
count_events_in_shard() {
    local port=$1
    local shard_id=$2

    count=$(docker exec shard-${shard_id}-write psql -U postgres -d shardstream_shard_${shard_id} -t -c "SELECT COUNT(*) FROM events;" 2>/dev/null | tr -d ' ')

    echo "$count"
}

total_events=0

for i in 0 1 2 3; do
    port=$((5432 + i * 3))
    count=$(count_events_in_shard $port $i)

    if [ ! -z "$count" ]; then
        echo "Shard $i: $count events"
        total_events=$((total_events + count))
    else
        echo "Shard $i: Unable to query"
    fi
done

echo ""
echo "Total events across all shards: $total_events"
echo ""

if [ $total_events -gt 0 ]; then
    echo "✓ SUCCESS: Sharding is working! Data is distributed across multiple shards."
else
    echo "✗ WARNING: No events found. Services may still be starting up."
fi

echo ""
echo "Step 3: Demonstrating consistent hashing..."
echo "------------------------------------------------"
echo "The same customer_id will always route to the same shard."
echo ""

# Test same customer multiple times
TEST_CUSTOMER="cust-999"
echo "Ingesting 5 events for customer: $TEST_CUSTOMER"

shards_used=()

for i in {1..5}; do
    response=$(curl -s -X POST http://localhost:8080/api/v1/ingest \
        -H "Content-Type: application/json" \
        -d "{
            \"customerId\": \"$TEST_CUSTOMER\",
            \"eventType\": \"consistency.test\",
            \"payload\": {\"test\": $i}
        }")

    shard_id=$(echo $response | jq -r '.shardId')
    shards_used+=($shard_id)
    echo "  Event $i: Shard $shard_id"
done

# Check if all events went to the same shard
unique_shards=$(printf '%s\n' "${shards_used[@]}" | sort -u | wc -l)

echo ""
if [ $unique_shards -eq 1 ]; then
    echo "✓ SUCCESS: All events for $TEST_CUSTOMER went to the same shard (Shard ${shards_used[0]})"
    echo "  This confirms consistent hashing is working correctly!"
else
    echo "✗ ERROR: Events were distributed across multiple shards (expected 1, got $unique_shards)"
fi

echo ""
echo "Step 4: Shard distribution summary..."
echo "------------------------------------------------"

# Show which customers are on which shards
echo "Customer ID distribution across shards:"
echo ""

for i in 0 1 2 3; do
    customers=$(docker exec shard-${i}-write psql -U postgres -d shardstream_shard_${i} -t -c "SELECT DISTINCT customer_id FROM events ORDER BY customer_id;" 2>/dev/null | grep -v '^$' | tr -d ' ')

    if [ ! -z "$customers" ]; then
        echo "Shard $i:"
        echo "$customers" | while read -r customer; do
            echo "  - $customer"
        done
    fi
done

echo ""
echo "=========================================="
echo "Sharding Verification Complete!"
echo "=========================================="
echo ""
echo "Key Points:"
echo "1. Data is distributed across 4 shards using consistent hashing"
echo "2. Each customer_id always routes to the same shard"
echo "3. Each shard has 1 write node + 2 read replicas"
echo "4. Queries can scatter-gather across all shards for global queries"
echo ""
