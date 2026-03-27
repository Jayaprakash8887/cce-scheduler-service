# CCE Scheduler Service — Flow Diagrams

All diagrams use Mermaid syntax for rendering in GitHub/IDE preview.

---

## 1. Scheduler Main Loop

The core scan-publish cycle that runs on every `fixedDelay` interval.

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
        alt Not leader
            SL->>Metrics: Record cycle (idle)
            Note over SL: Skip - standby mode
        else Is leader
            SL->>Scanner: scan(batchSize)
            Scanner->>DB: SELECT step_instance<br/>WHERE state/date thresholds met<br/>ORDER BY threshold ASC<br/>LIMIT batchSize
            DB-->>Scanner: List of StepInstance
            Scanner-->>SL: List of DueStep

            alt Empty batch
                SL->>Metrics: Record cycle (empty)
            else Steps found
                loop For each DueStep
                    SL->>Publisher: publish(dueStep)
                    Publisher->>Kafka: Send SchedulerTriggerMessage<br/>key=protocolInstanceId
                    Kafka-->>Publisher: Ack
                    Publisher->>Metrics: publish.success++
                end
            end

            SL->>Leader: updateHeartbeat()
            Leader->>DB: UPDATE scheduler_lease<br/>SET last_heartbeat=now()
            SL->>Metrics: Record cycle (scan duration, batch size)
        end
    end
```

---

## 2. Leader Election Lifecycle

```mermaid
flowchart TD
    A["Service Startup"] --> B["Create dedicated<br/>JDBC connection"]
    B --> C{"pg_try_advisory_lock<br/>(lockKey)?"}
    C -->|"Acquired"| D["leader = true"]
    C -->|"Not acquired"| E["leader = false<br/>(standby)"]

    D --> F["Update scheduler_lease<br/>leader_id, last_heartbeat"]
    F --> G["Start scan loop"]

    E --> H["Wait leaderRetryInterval"]
    H --> C

    G --> I{"Scan cycle<br/>completed?"}
    I -->|"Yes"| J["Update heartbeat"]
    J --> K["Wait fixedDelay"]
    K --> I

    subgraph Shutdown
        L["@PreDestroy"] --> M["Release advisory lock"]
        M --> N["Close dedicated connection"]
    end

    subgraph Failover
        O["Leader dies"] --> P["PG connection drops"]
        P --> Q["Advisory lock released<br/>automatically"]
        Q --> R["Standby acquires lock<br/>on next retry"]
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
    A["DueStepScanner.scan()"] --> B["Query PostgreSQL"]
    B --> C["SELECT steps WHERE<br/>(PENDING AND dueDate ≤ now) OR<br/>(DUE AND overdueDate ≤ now) OR<br/>(OVERDUE AND missedDate ≤ now)<br/>ORDER BY threshold ASC<br/>LIMIT batchSize"]
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
    L -->|"No"| M["Return List<DueStep>"]

    style A fill:#4A90D9,color:white
    style M fill:#27AE60,color:white
```

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

## 5. Failover Scenario

```mermaid
sequenceDiagram
    participant L1 as Instance 1 (Leader)
    participant L2 as Instance 2 (Standby)
    participant PG as PostgreSQL
    participant Kafka as Kafka

    Note over L1,PG: Normal operation
    L1->>PG: Hold advisory lock
    L1->>PG: Scan step_instance
    L1->>Kafka: Publish transitions

    Note over L1: Instance 1 crashes
    L1--xPG: Connection drops
    Note over PG: Advisory lock auto-released

    L2->>PG: pg_try_advisory_lock()
    PG-->>L2: Lock acquired!
    Note over L2: Becomes leader

    L2->>PG: Update scheduler_lease<br/>leader_id = instance-2
    L2->>PG: Scan step_instance
    L2->>Kafka: Publish transitions

    Note over L2,Kafka: Compliance Service handles<br/>any duplicate messages<br/>idempotently
```
