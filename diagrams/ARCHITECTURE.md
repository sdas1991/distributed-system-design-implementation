# ShardStream Architecture Diagrams

## System Architecture

```mermaid
graph TB
    Client[Client Applications]

    Client --> Gateway[API Gateway<br/>Port 8080<br/>Rate Limiting & Load Balancing]

    Gateway --> Ingest[Ingest Service<br/>Port 8081<br/>Kotlin/Ktor]
    Gateway --> Query[Query Service<br/>Port 8082<br/>Kotlin/Ktor]
    Gateway -.WebSocket.-> Realtime[Realtime Service<br/>Port 8083<br/>Python/FastAPI]

    Ingest --> NATS[NATS JetStream<br/>Event Bus<br/>Port 4222]
    NATS --> Realtime
    NATS --> Batch[Batch Service<br/>Python]

    Ingest --> Shard0W[(Shard 0<br/>Write)]
    Ingest --> Shard1W[(Shard 1<br/>Write)]
    Ingest --> Shard2W[(Shard 2<br/>Write)]
    Ingest --> Shard3W[(Shard 3<br/>Write)]

    Query --> Shard0R1[(Shard 0<br/>Read-1)]
    Query --> Shard0R2[(Shard 0<br/>Read-2)]
    Query --> Shard1R1[(Shard 1<br/>Read-1)]
    Query --> Shard1R2[(Shard 1<br/>Read-2)]
    Query --> Shard2R1[(Shard 2<br/>Read-1)]
    Query --> Shard2R2[(Shard 2<br/>Read-2)]
    Query --> Shard3R1[(Shard 3<br/>Read-1)]
    Query --> Shard3R2[(Shard 3<br/>Read-2)]

    Batch --> Shard0W
    Batch --> Shard1W
    Batch --> Shard2W
    Batch --> Shard3W

    Realtime -.push.-> Client

    style Gateway fill:#ff9999
    style NATS fill:#99ccff
    style Ingest fill:#99ff99
    style Query fill:#99ff99
    style Realtime fill:#ffff99
    style Batch fill:#ffff99
```

## Shard Topology

```mermaid
graph LR
    subgraph "Shard 0"
        S0W[(Write Node<br/>Port 5432)]
        S0R1[(Read Replica 1<br/>Port 5433)]
        S0R2[(Read Replica 2<br/>Port 5434)]
        S0W -.replication.-> S0R1
        S0W -.replication.-> S0R2
    end

    subgraph "Shard 1"
        S1W[(Write Node<br/>Port 5435)]
        S1R1[(Read Replica 1<br/>Port 5436)]
        S1R2[(Read Replica 2<br/>Port 5437)]
        S1W -.replication.-> S1R1
        S1W -.replication.-> S1R2
    end

    subgraph "Shard 2"
        S2W[(Write Node<br/>Port 5438)]
        S2R1[(Read Replica 1<br/>Port 5439)]
        S2R2[(Read Replica 2<br/>Port 5440)]
        S2W -.replication.-> S2R1
        S2W -.replication.-> S2R2
    end

    subgraph "Shard 3"
        S3W[(Write Node<br/>Port 5441)]
        S3R1[(Read Replica 1<br/>Port 5442)]
        S3R2[(Read Replica 2<br/>Port 5443)]
        S3W -.replication.-> S3R1
        S3W -.replication.-> S3R2
    end

    Router[Shard Router<br/>Consistent Hashing<br/>MD5 Hash modulo 4]

    Router --> S0W
    Router --> S1W
    Router --> S2W
    Router --> S3W

    style Router fill:#ff9999
```

## Event-Driven Pipelines

### Real-time Pipeline

```mermaid
sequenceDiagram
    participant Client
    participant Gateway
    participant Ingest
    participant DB as Shard DB
    participant NATS
    participant Realtime
    participant WS as WebSocket Client

    Client->>Gateway: POST /api/v1/ingest
    Gateway->>Ingest: Forward request

    Ingest->>DB: Write to shard (based on customer_id)
    Ingest->>NATS: Publish to ingest.raw
    Ingest->>NATS: Publish to updates.realtime
    Ingest-->>Gateway: 201 Created
    Gateway-->>Client: Response

    NATS->>Realtime: Consume updates.realtime
    Realtime->>WS: Push event via WebSocket

    Note over Realtime,WS: Real-time latency: < 100ms
```

### Delayed Pipeline

```mermaid
sequenceDiagram
    participant Ingest
    participant NATS
    participant Batch
    participant DB as Shard DB

    Ingest->>NATS: Publish to updates.delayed

    Note over NATS,Batch: Wait 5 minutes (delayed processing)

    NATS->>Batch: Consume delayed event
    Batch->>DB: Update processed_at timestamp
    Batch->>DB: Aggregate into metrics_hourly

    Note over Batch,DB: Batch processing for optimization
```

## Data Flow

```mermaid
flowchart TD
    A[Incoming Event] -->|customer_id: cust-123| B{Shard Router}
    B -->|MD5 hash mod 4| C[Calculate Shard ID]

    C -->|Shard 0| D0[Write to Shard 0]
    C -->|Shard 1| D1[Write to Shard 1]
    C -->|Shard 2| D2[Write to Shard 2]
    C -->|Shard 3| D3[Write to Shard 3]

    D0 --> E[Publish to Event Bus]
    D1 --> E
    D2 --> E
    D3 --> E

    E --> F[Real-time Stream]
    E --> G[Delayed Stream]

    F --> H[WebSocket Push]
    G --> I[Batch Processing]

    I --> J[Hourly Aggregation]
    I --> K[Data Compaction]
```

## Scatter-Gather Query Pattern

```mermaid
sequenceDiagram
    participant Client
    participant Query as Query Service
    participant S0 as Shard 0 Read
    participant S1 as Shard 1 Read
    participant S2 as Shard 2 Read
    participant S3 as Shard 3 Read

    Client->>Query: GET /query/events/range?startTime=X&endTime=Y

    par Query All Shards
        Query->>S0: SELECT events WHERE time BETWEEN X AND Y
        Query->>S1: SELECT events WHERE time BETWEEN X AND Y
        Query->>S2: SELECT events WHERE time BETWEEN X AND Y
        Query->>S3: SELECT events WHERE time BETWEEN X AND Y
    end

    S0-->>Query: Results from Shard 0
    S1-->>Query: Results from Shard 1
    S2-->>Query: Results from Shard 2
    S3-->>Query: Results from Shard 3

    Query->>Query: Merge & Sort Results
    Query-->>Client: Combined Results

    Note over Query: Parallel queries for performance
```

## Resilience Patterns

```mermaid
graph TB
    Request[Incoming Request]

    Request --> CB{Circuit Breaker<br/>State?}

    CB -->|Closed| Retry[Retry Logic<br/>Max 3 attempts<br/>Exponential backoff]
    CB -->|Open| Fail[Fail Fast<br/>Return Error]
    CB -->|Half-Open| Test[Test Request]

    Retry --> Exec[Execute Request]

    Exec -->|Success| Success[Return Result]
    Exec -->|Failure| CheckRetry{Retry<br/>Attempts<br/>Exhausted?}

    CheckRetry -->|No| Retry
    CheckRetry -->|Yes| OpenCircuit[Open Circuit Breaker]

    OpenCircuit --> Fail

    Test -->|Success| CloseCircuit[Close Circuit Breaker]
    Test -->|Failure| Fail

    style CB fill:#ff9999
    style Retry fill:#99ff99
    style Exec fill:#99ccff
```

## Horizontal Scaling

```mermaid
graph TB
    LB[Load Balancer]

    subgraph "Ingest Service Instances"
        I1[Ingest-1]
        I2[Ingest-2]
        I3[Ingest-N]
    end

    subgraph "Query Service Instances"
        Q1[Query-1]
        Q2[Query-2]
        Q3[Query-N]
    end

    subgraph "Realtime Service Instances"
        R1[Realtime-1]
        R2[Realtime-2]
        R3[Realtime-N]
    end

    LB --> I1
    LB --> I2
    LB --> I3

    LB --> Q1
    LB --> Q2
    LB --> Q3

    LB --> R1
    LB --> R2
    LB --> R3

    I1 --> Shards[(Sharded Database)]
    I2 --> Shards
    I3 --> Shards

    Q1 --> Shards
    Q2 --> Shards
    Q3 --> Shards

    I1 --> NATS[NATS Event Bus]
    I2 --> NATS
    I3 --> NATS

    NATS --> R1
    NATS --> R2
    NATS --> R3

    style LB fill:#ff9999
    style NATS fill:#99ccff
```
