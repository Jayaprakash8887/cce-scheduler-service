# CCE Scheduler Service — Flow Diagrams

All diagrams use Mermaid syntax for rendering in GitHub/IDE preview.

---

## 1. Scheduler Main Loop

The core scan-publish cycle that runs on every `fixedDelay` interval. Each instance scans only its assigned partition.

```mermaid
sequenceDiagram
    participant SL as SchedulerLoop<br/>(@Scheduled)
    participant Leader as LeaderElection
    participant Scanner as DueStepScanner
    participant DB as PostgreSQL
    participant Publisher as TransitionPublisher
    participant Kafka as Apache Kafka
    participant Metrics as Micrometer

    loop fixedDelay (5s default)
        SL->>Leader: isLeader()?
        alt Not a partition leader
            SL->>Metrics: Record cycle (idle)
            Note over SL: Skip - standby mode
        else Is partition leader (partitionIndex=P)
            SL->>Scanner: scan(batchSize, P, totalPartitions)
            Scanner->>DB: SELECT step_instance<br/>WHERE state/date thresholds met<br/>AND MOD(ABS(HASHTEXT(protocol_instance_id)), N) = P<br/>ORDER BY threshold ASC<br/>LIMIT batchSize
            DB-->>Scanner: List of StepInstance
            Scanner-->>SL: List of DueStep

            alt Empty batch
                SL->>Metrics: Record cycle (empty, partition=P)
            else Steps found
                loop For each DueStep
                    SL->>Publisher: publish(dueStep)
                    Publisher->>Kafka: Send SchedulerTriggerMessage<br/>key=protocolInstanceId
                    Kafka-->>Publisher: Ack
                    Publisher->>Metrics: publish.success++ (partition=P)
                end
            end

            SL->>Leader: updateHeartbeat(P)
            Leader->>DB: UPDATE scheduler_lease<br/>SET last_heartbeat=now()<br/>WHERE partition_index=P
            SL->>Metrics: Record cycle (scan duration, batch size, partition=P)
        end
    end
```

---

## 2. Partitioned Leader Election Lifecycle

```mermaid
flowchart TD
    A["Service Startup"] --> B["Create dedicated<br/>JDBC connection"]
    B --> C{"For i in 0..totalPartitions-1:<br/>pg_try_advisory_lock<br/>(lockKey + i)?"}
    C -->|"Acquired lock i"| D["partitionIndex = i<br/>leader = true"]
    C -->|"No lock acquired"| E["leader = false<br/>(standby)"]

    D --> F["Update scheduler_lease<br/>row for partition i"]
    F --> G["Start scan loop<br/>(partition-filtered)"]

    E --> H["Wait leaderRetryInterval"]
    H --> C

    G --> I{"Scan cycle<br/>completed?"}
    I -->|"Yes"| J["Update heartbeat<br/>for partition i"]
    J --> K["Wait fixedDelay"]
    K --> I

    subgraph Shutdown
        L["@PreDestroy"] --> M["Release advisory lock i"]
        M --> N["Close dedicated connection"]
    end

    subgraph Failover
        O["Partition leader dies"] --> P["PG connection drops"]
        P --> Q["Advisory lock i released<br/>automatically"]
        Q --> R["Standby acquires lock i<br/>on next retry"]
        R --> D
    end

    style D fill:#27AE60,color:white
    style E fill:#E67E22,color:white
    style Q fill:#E74C3C,color:white
    style R fill:#27AE60,color:white
```

---

## 3. Due Step Scanning Algorithm

```mermaid
flowchart TD
    A["DueStepScanner.scan(batchSize, P, N)"] --> B["Query PostgreSQL"]
    B --> C["SELECT steps WHERE<br/>(PENDING AND dueDate ≤ now) OR<br/>(DUE AND overdueDate ≤ now) OR<br/>(OVERDUE AND missedDate ≤ now)<br/>AND MOD(ABS(HASHTEXT(protocol_instance_id::text)), N) = P<br/>ORDER BY threshold ASC<br/>LIMIT batchSize"]
    C --> D{"Results empty?"}
    D -->|"Yes"| E["Return empty list"]
    D -->|"No"| F["For each StepInstance"]

    F --> G{"Current state?"}
    G -->|"PENDING"| H["transitionType = PENDING_TO_DUE"]
    G -->|"DUE"| I["transitionType = DUE_TO_OVERDUE"]
    G -->|"OVERDUE"| J["transitionType = OVERDUE_TO_MISSED"]

    H --> K["Create DueStep record"]
    I --> K
    J --> K

    K --> L{"More steps?"}
    L -->|"Yes"| F
    L -->|"No"| M["Return List of DueStep"]

    style A fill:#4A90D9,color:white
    style M fill:#27AE60,color:white
```

> **P** = this instance’s `partitionIndex`, **N** = `totalPartitions`. When N=1, the `MOD(...)` clause is always 0 = P, effectively a no-op (full table scan).

---

## 4. State Transition Determination

```mermaid
graph LR
    subgraph "Time-Based (Scheduler)"
        P["PENDING"] -->|"dueDate ≤ now"| D["DUE"]
        D -->|"overdueDate ≤ now"| O["OVERDUE"]
        O -->|"missedDate ≤ now"| M["MISSED"]
    end

    subgraph "Event-Based (Compliance Service)"
        P2["PENDING"] -->|"Event match"| C["COMPLETED"]
        D2["DUE"] -->|"Event match"| C2["COMPLETED"]
        O2["OVERDUE"] -->|"Event match"| C3["COMPLETED"]
        O3["OVERDUE"] -->|"Optional step<br/>auto-skip"| S["SKIPPED"]
    end

    style P fill:#3498DB,color:white
    style D fill:#F39C12,color:white
    style O fill:#E74C3C,color:white
    style M fill:#7F8C8D,color:white
    style C fill:#27AE60,color:white
    style C2 fill:#27AE60,color:white
    style C3 fill:#27AE60,color:white
    style S fill:#95A5A6,color:white
```

---

## 5. Failover Scenario (Partitioned)

```mermaid
sequenceDiagram
    participant L1 as Instance A (Partition 0)
    participant L2 as Instance B (Partition 1)
    participant L3 as Instance C (Standby)
    participant PG as PostgreSQL
    participant Kafka as Kafka

    Note over L1,L2: Normal operation — 2 partitions active
    L1->>PG: Hold advisory lock 100001 (partition 0)
    L2->>PG: Hold advisory lock 100002 (partition 1)
    L1->>PG: Scan step_instance (partition 0)
    L2->>PG: Scan step_instance (partition 1)
    L1->>Kafka: Publish transitions (partition 0)
    L2->>Kafka: Publish transitions (partition 1)

    Note over L1: Instance A crashes
    L1--xPG: Connection drops
    Note over PG: Advisory lock 100001 auto-released

    L3->>PG: pg_try_advisory_lock(100001)
    PG-->>L3: Lock acquired!
    Note over L3: Becomes partition 0 leader

    L3->>PG: Update scheduler_lease<br/>partition_index=0, leader_id=C
    L3->>PG: Scan step_instance (partition 0)
    L3->>Kafka: Publish transitions (partition 0)

    Note over L2,L3: Both partitions active again
    Note over Kafka: Compliance Service handles<br/>any duplicate messages<br/>idempotently
```
