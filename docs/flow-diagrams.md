# CCE Scheduler Service — Flow Diagrams

Visual reference for the scheduler's runtime behavior: the main scan-publish loop, single-leader election, due-step scanning, state transitions, and failover.

---

## 1. Scheduler Main Loop

The core scan-publish cycle that runs on every `fixedDelay` interval. Only the instance holding the leader lock scans; the rest stand by.

```mermaid
sequenceDiagram
    participant SL as SchedulerLoop
    participant Leader as LeaderElection
    participant Scanner as DueStepScanner
    participant DB as PostgreSQL
    participant Publisher as TransitionPublisher
    participant Kafka as Kafka
    participant Metrics as Metrics

    loop Every fixedDelay (default 10s)
        SL->>Leader: isLeader()?
        alt Not leader
            Note over SL: Standby — skip cycle
        else Leader
            SL->>Scanner: scan()
            Scanner->>DB: SELECT watermark, watermark_id FROM scheduler_scan_cursor (epoch + min-UUID if none)
            Scanner->>DB: SELECT step_instance<br/>WHERE state/date thresholds met<br/>AND (threshold, id) &gt; (watermark, watermark_id)<br/>ORDER BY threshold ASC, id ASC<br/>LIMIT batchSize
            DB-->>Scanner: List of StepInstance
            Scanner-->>SL: List of DueStep

            alt Steps found
                SL->>Publisher: publishAll(dueSteps)
                Publisher->>Kafka: Fire all sends async (key=protocolInstanceId)
                Kafka-->>Publisher: Acks (awaited as a batch)
                Publisher->>Metrics: publish.success / publish.failure
                opt All published successfully
                    SL->>DB: UPSERT scheduler_scan_cursor<br/>SET (watermark, watermark_id) = last emitted (threshold, id)<br/>WHERE new &gt; old (monotonic)
                end
            end

            SL->>Metrics: Record cycle + scan duration
        end
    end
```

---

## 2. Single-Leader Election Lifecycle

One PostgreSQL advisory lock decides the single active leader; all other instances stand by and retry.

```mermaid
flowchart TD
    A["tryAcquireLeadership()<br/>(every leaderRetryInterval)"] --> B["Ensure dedicated JDBC connection<br/>(fresh connection holds no lock)"]
    B --> C{"Already hold the lock<br/>on this connection?"}
    C -->|"Yes"| LEAD
    C -->|"No"| D["pg_try_advisory_lock(advisoryLockKey)"]
    D --> E{"Acquired?"}
    E -->|"Yes"| LEAD["LEADER:<br/>update scheduler_lease heartbeat + lease expiry"]
    E -->|"No"| STBY["STANDBY:<br/>lock held elsewhere — do nothing this cycle"]

    LEAD --> W["Wait leaderRetryInterval → repeat"]
    STBY --> W

    style LEAD fill:#27AE60,color:white
    style STBY fill:#7F8C8D,color:white
```

**Failover:** when the leader dies, its dedicated connection drops and PostgreSQL releases the advisory lock. A standby's next `pg_try_advisory_lock` succeeds (≤ `leaderRetryInterval`), promoting it. A `holdsLock` flag is reset whenever the connection is (re)established or dropped, so a silently-dropped connection can never leave a stale "still leader" belief.

---

## 3. Due Step Scanning Algorithm

```mermaid
flowchart TD
    A["DueStepScanner.scan()"] --> A2["Read cursor (W, Wid)<br/>(epoch + min-UUID if no row)"]
    A2 --> B["Query PostgreSQL (whole table)"]
    B --> C["SELECT steps WHERE<br/>threshold ≤ now AND (threshold, id) > (W, Wid)<br/>evaluated per state (PENDING→dueDate,<br/>DUE→overdueDate, OVERDUE→missedDate)<br/>ORDER BY threshold ASC, id ASC<br/>LIMIT batchSize"]
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

> **(W, Wid)** = the keyset scan cursor `(threshold, step id)`. It is the exclusive lower bound in `(threshold, id)` order: the scan skips crossings already emitted in a prior cycle, and the UUID v7 `id` tie-breaker lets a same-timestamp cohort larger than `batch-size` drain across cycles (in creation order) rather than being truncated. `SchedulerLoop` advances the cursor to the last emitted `(threshold, id)` after a fully successful publish.

### Indexing & Query Optimization Notes

`step_instance` is owned by the Compliance Service; the Scheduler reads it. To keep the watermark-bounded scan efficient as the table grows, the ideal supporting indexes are **partial indexes on each threshold column with `id` as a trailing column**, so the `(threshold, id)` keyset seek is fully index-ordered:

```sql
CREATE INDEX idx_step_instance_pending_due   ON step_instance (due_date, id)     WHERE state = 'PENDING';
CREATE INDEX idx_step_instance_due_overdue   ON step_instance (overdue_date, id) WHERE state = 'DUE';
CREATE INDEX idx_step_instance_overdue_missed ON step_instance (missed_date, id) WHERE state = 'OVERDUE';
```

These are created by migration `V6__step_instance_scan_indexes.sql` (guarded by a table-exists check, `CREATE INDEX IF NOT EXISTS`, so they coexist with any Compliance-side indexes). UUID v7 ids are sequential, so they stay compact (append-mostly inserts). Verify efficiency after deploy with `EXPLAIN (ANALYZE, BUFFERS)` on the scan query.

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

The Scheduler only ever *requests* the time-based transitions (left). The Compliance Service is the sole writer of `step_instance.state` and decides the terminal outcome (MISSED vs SKIPPED by `requiredBehavior`, or COMPLETED on an event match).

---

## 5. Failover Scenario

```mermaid
sequenceDiagram
    participant L1 as Instance A (leader)
    participant L2 as Instance B (standby)
    participant PG as PostgreSQL
    participant Kafka as Kafka

    Note over L1,L2: Normal operation — A is leader, B stands by
    L1->>PG: Holds advisory lock (advisoryLockKey)
    L1->>PG: Scan whole table (watermark-bounded)
    L1->>Kafka: Publish transitions (async batched)
    L2->>PG: pg_try_advisory_lock → false (held by A) → standby

    Note over L1: Instance A crashes
    L1--xPG: Connection drops
    Note over PG: Advisory lock auto-released

    Note over L2: Next retry (≤ leaderRetryInterval)
    L2->>PG: pg_try_advisory_lock(advisoryLockKey)
    PG-->>L2: Lock acquired!
    Note over L2: B is now the leader

    L2->>PG: Scan whole table
    L2->>Kafka: Publish transitions
    Note over Kafka: Compliance Service handles<br/>any duplicate messages idempotently
```
