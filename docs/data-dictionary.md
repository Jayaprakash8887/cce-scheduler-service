# CCE Scheduler Service — Data Dictionary

Comprehensive reference for all database tables, entities, enums, Kafka message fields, configuration properties, and metrics used by the Scheduler Service.

---

## 1. Database Tables

### 1.1 `scheduler_lease` — Leader Heartbeat

Holds the current leader's heartbeat and lease expiry as a **single row keyed by `id = 0`** (upserted by the leader). The advisory lock — not this row — is the actual leadership mechanism; the row is write-only observability (the health indicator reads in-memory state, not this table).

**Migration:** `V1__create_scheduler_lease.sql`  
**Owner:** Scheduler Service (read-write)

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `INTEGER` | No | — | Primary key — always `0` (single row). |
| `leader_id` | `VARCHAR` | Yes | — | Instance ID (per-process UUID) of the current leader. `NULL` when no leader is active. |
| `last_heartbeat` | `TIMESTAMPTZ` | Yes | — | Last time the leader upserted this row. |
| `lease_expires_at` | `TIMESTAMPTZ` | Yes | — | When the current lease expires. Computed as `last_heartbeat + leaseDurationSeconds`. |

**Constraints:**
- `PK`: `id`

```sql
CREATE TABLE scheduler_lease (
    id               INTEGER PRIMARY KEY,
    leader_id        VARCHAR,
    last_heartbeat   TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
);
```

### 1.2 `scheduler_scan_cursor` — Scan Watermark

A **single-row** table (`id = 0`), holding the scan cursor as a **keyset/seek cursor over the composite key `(threshold, step_id)`** — the exclusive lower bound of the scan window. The scan considers only `step_instance` rows that sort **strictly after** `(watermark, watermark_id)` (i.e. `eff_threshold > watermark`, or `eff_threshold = watermark AND id > watermark_id`), and `SchedulerLoop` advances the cursor to the **last emitted** `(threshold, id)` after a **fully successful** publish cycle. This stops the scheduler from re-selecting and re-publishing the same threshold crossing on every cycle while a step's state is frozen (e.g., while the Compliance Service is lagging or down), which is the primary cause of duplicate events in `cce.scheduler.triggers`.

The `id` component (a **UUID v7** from the Compliance Service, as of its v4→v7 change) breaks threshold ties, so a cohort of steps sharing an **identical threshold** (calendar-date schedules, bulk backfills) drains across cycles `batch-size` at a time — no truncation, no stall — and, because v7 is time-ordered by creation, in creation (FIFO) order. Note the id is only a **tie-breaker under `threshold`**, never the primary cursor: a v7 id encodes *creation* time, not the due/missed threshold, so ordering by id alone would strand later-due-but-earlier-created steps. Correctness needs only a **total order** over `id`, which Postgres's `uuid` type provides for any version — so pre-change **v4** rows and post-change **v7** rows coexist safely (the FIFO-within-cohort property just applies to the v7 rows).

Written only by the **current leader** (serialized by the advisory lock), so there is no cross-writer contention. The upsert's `WHERE ROW(new) > ROW(old)` guard makes the write monotonic in lexical `(timestamp, id)` order — a stale/clock-skewed value from a new owner after failover can never move the cursor backwards.

**Migration:** `V2__scheduler_scan_cursor.sql`  
**Owner:** Scheduler Service (read-write)

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `INTEGER` | No | — | Primary key — always `0` (single cursor row). |
| `watermark` | `TIMESTAMPTZ` | No | — | Timestamp component of the cursor (full microsecond precision). No cursor row ⇒ treated as the epoch (first scan sees the full due backlog up to now). |
| `watermark_id` | `UUID` | No | `00000000-…-000000000000` | Id component — the `step_instance.id` (UUID v7) of the last emitted step at `watermark`. The min-UUID default sorts before every real id. |
| `updated_at` | `TIMESTAMPTZ` | No | `now()` | Last time the cursor advanced (observability). |

**Constraints:**
- `PK`: `id`

```sql
CREATE TABLE scheduler_scan_cursor (
    id           INTEGER PRIMARY KEY,
    watermark    TIMESTAMPTZ NOT NULL,
    watermark_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

> **Known gaps (accepted; addressed elsewhere):** (a) a step whose `(threshold, id)` sorts **below** the current cursor — backfill / backdated start / clock skew, or a late crossing at exactly the `watermark` instant with `id < watermark_id` — is never scanned; (b) a trigger published to Kafka but never applied by the Compliance Service is not re-driven (transient publish **failures** *are* retried, because the cursor advances only when every trigger in the cycle succeeds). The former same-timestamp-cohort truncation is **resolved** by the `(threshold, id)` keyset cursor.

### 1.3 `step_instance` — Read-Only View (Owned by Compliance Service)

The Scheduler reads this table to find steps that need time-based transitions. **The Scheduler never writes to this table.**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | `UUID` | No | Primary key |
| `protocol_instance_id` | `UUID` | No | FK to `protocol_instance` |
| `action_id` | `VARCHAR` | No | PlanDefinition action ID (e.g., `anc-visit-1`) |
| `repeat_index` | `INTEGER` | No | 0-based recurrence index |
| `state` | `VARCHAR` | No | Current state — see `StepState` enum |
| `due_date` | `TIMESTAMPTZ` | Yes | When the step becomes DUE |
| `overdue_date` | `TIMESTAMPTZ` | Yes | Legacy column (due_date + tolerance). **Not read by the Scheduler** — the `DUE → OVERDUE` transition has been removed. |
| `missed_date` | `TIMESTAMPTZ` | Yes | When the step becomes MISSED |
| `completed_at` | `TIMESTAMPTZ` | Yes | When the step was completed (event-driven) |
| `completed_by_source` | `VARCHAR` | Yes | Source system that completed the step |
| `completion_status` | `VARCHAR` | Yes | `EARLY`, `ON_TIME`, or `LATE` |
| `matched_event_id` | `UUID` | Yes | event_log ID of the completing event |
| `required_behavior` | `VARCHAR` | Yes | FHIR `requiredBehavior` code: `must`, `could`, or `must-unless-documented`. Determines MISSED vs SKIPPED on `DUE_TO_MISSED`. |
| `created_at` | `TIMESTAMPTZ` | No | When the step was instantiated |
| `updated_at` | `TIMESTAMPTZ` | No | Last state change |

**Scheduler Query Interest:** Only columns `id`, `protocol_instance_id`, `state`, `due_date`, `missed_date`, `required_behavior` are used by the Scheduler's scan query.

---

## 2. Enum Values

### 2.1 `StepState`

State of a step instance in its lifecycle. The Scheduler only queries for `PENDING` and `DUE` states.

| Value | Description | Scheduler Relevant? |
|-------|-------------|---------------------|
| `PENDING` | Step created but not yet due | Yes — transitions to `DUE` when `due_date ≤ now` |
| `DUE` | Step is now actionable | Yes — transitions to `MISSED` when `missed_date ≤ now` |
| `OVERDUE` | **Legacy — no longer part of the lifecycle.** The `DUE → OVERDUE` transition has been removed; the Scheduler never scans this state and never moves a step into it. Rows written before the change may still carry it. | No |
| `MISSED` | Step was never completed (terminal) | No — terminal state |
| `COMPLETED` | Step was completed by an event (terminal) | No — terminal state |
| `SKIPPED` | Step was skipped (optional steps only) | No — terminal state |

### 2.2 `TransitionType`

The type of time-based transition the Scheduler requests.

| Value | From State | To State | Condition |
|-------|-----------|----------|-----------|
| `PENDING_TO_DUE` | `PENDING` | `DUE` | `due_date ≤ now` |
| `DUE_TO_MISSED` | `DUE` | `MISSED` or `SKIPPED` | `missed_date ≤ now`. Compliance Service applies `MISSED` for `must` steps (with deviation) or `SKIPPED` for `could` steps (no deviation). |

---

## 3. Kafka Message Fields

### 3.1 `SchedulerTriggerMessage`

Published to `cce.scheduler.triggers` topic.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `stepInstanceId` | `UUID` (String) | No | The `step_instance.id` that needs a state transition |
| `transitionType` | `String` | No | One of: `PENDING_TO_DUE`, `DUE_TO_MISSED` |
| `triggeredAt` | `OffsetDateTime` (ISO 8601) | No | When the scan cycle detected the threshold crossing |
| `correlationid` | `String` | No | Distributed tracing ID — format: `sched-{transitionType}-{stepId-prefix}` (lowercase per CloudEvents convention) |

---

## 4. Internal Records

### 4.1 `DueStep`

Internal record returned by `DueStepScanner` — not persisted or published.

| Field | Type | Description |
|-------|------|-------------|
| `stepInstanceId` | `UUID` | The step that needs transition |
| `protocolInstanceId` | `UUID` | Owning protocol instance (used as Kafka key) |
| `transitionType` | `TransitionType` | Computed transition |
| `thresholdDate` | `OffsetDateTime` | The date threshold that was crossed |
| `metadata` | `Map<String, Object>` | Additional context (e.g., days overdue) |

---

## 5. Configuration Properties

All properties are under the `cce.scheduler` prefix.

| Property | Type | Default | Min | Max | Description |
|----------|------|---------|-----|-----|-------------|
| `scan-interval` | `long` (ms) | `10000` | `1000` | `300000` | Delay between scan cycles (`fixedDelay`) |
| `batch-size` | `int` | `1000` | `1` | `1000` | Max steps fetched (and published) per scan cycle |
| `lease-duration-seconds` | `int` | `30` | `10` | `300` | Lease expiry recorded in the leader heartbeat |
| `leader-retry-interval` | `long` (ms) | `5000` | `1000` | `60000` | How often an instance (re)attempts the advisory lock |
| `advisory-lock-key` | `long` | `100001` | — | — | PostgreSQL advisory lock key for single-leader election |

> The watermark is core functionality and always on — there is no flag to disable it.

---

## 6. Metrics

| Metric Name | Type | Tags | Description |
|-------------|------|------|-------------|
| `cce.scheduler.scan.duration` | Timer | — | Time spent per scan cycle (query + publish) |
| `cce.scheduler.scan.steps` | Counter | `transition_type` | Number of steps found per transition type |
| `cce.scheduler.publish.success` | Counter | `transition_type` | Successful Kafka publishes per type |
| `cce.scheduler.publish.failure` | Counter | `transition_type` | Failed Kafka publishes per type |
| `cce.scheduler.leader.status` | Gauge | — | `1` = leader, `0` = standby |
| `cce.scheduler.cycle.count` | Counter | — | Total scan cycles executed (including empty ones) |
