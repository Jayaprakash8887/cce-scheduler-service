# CCE Scheduler Service — Kafka Events

Detailed reference for the Kafka topic produced by the Scheduler Service, message schema, sample payloads, and producer configuration.

---

## 1. Topic Overview

The Scheduler Service produces to exactly **one** Kafka topic. It does not consume from any topic.

| Property | Value |
|----------|-------|
| **Topic** | `cce.scheduler.triggers` |
| **Direction** | Outbound (Scheduler → Compliance Service) |
| **Message Key** | `protocolInstanceId` (UUID string) — ensures all transitions for a protocol instance go to the same partition |
| **Value Format** | JSON (`SchedulerTriggerMessage`) |
| **Partitions** | 25 (matches Compliance Service topic configuration) |
| **Replication Factor** | 3 (production) / 1 (local dev) |
| **Guarantees** | At-least-once delivery (idempotent producer, synchronous publish). Partition-safe — all transitions for a `protocolInstanceId` are always produced by the same Scheduler instance (deterministic hash partitioning). |

---

## 2. Producer Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all
      retries: 3
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

| Setting | Value | Rationale |
|---|---|---|
| `acks` | `all` | Wait for all in-sync replicas to acknowledge |
| `retries` | `3` | Retry on transient failures |
| `enable.idempotence` | `true` | Exactly-once semantics within a partition |
| `max.in.flight.requests.per.connection` | `5` | Max allowed with idempotent producer |

**Publishing mode:** Synchronous — the Scheduler waits for broker acknowledgment before proceeding to the next step in the batch. This ensures ordering and simplifies error handling.

---

## 3. Message Schema — SchedulerTriggerMessage

```json
{
  "stepInstanceId": "770e8400-e29b-41d4-a716-446655440002",
  "transitionType": "DUE_TO_OVERDUE",
  "triggeredAt": "2026-03-25T00:00:00Z",
  "correlationid": "sched-DUE_TO_OVERDUE-770e8400"
}
```

### Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `stepInstanceId` | UUID (String) | Yes | The `step_instance.id` that needs a state transition |
| `transitionType` | String (enum) | Yes | One of: `PENDING_TO_DUE`, `DUE_TO_OVERDUE`, `OVERDUE_TO_MISSED` |
| `triggeredAt` | OffsetDateTime (ISO 8601) | Yes | Timestamp when the scan cycle detected the threshold crossing |
| `correlationid` | String | Yes | Distributed tracing ID — format: `sched-{transitionType}-{stepId-prefix}` |

### Transition Types

| Value | Meaning | Compliance Service Effect |
|---|---|---|
| `PENDING_TO_DUE` | Step's `dueDate` has been reached | Update `step_instance.state` from `PENDING` to `DUE` |
| `DUE_TO_OVERDUE` | Step's `overdueDate` has been reached | Update state to `OVERDUE`; create `deviation` record (type: `OVERDUE`); evaluate intelligence actions |
| `OVERDUE_TO_MISSED` | Step's `missedDate` has been reached | For `must` steps: update state to `MISSED`, create `deviation` record (type: `MISSED`), evaluate intelligence actions. For `could` steps: update state to `SKIPPED` (no deviation). The Compliance Service decides based on `requiredBehavior`. |

---

## 4. Sample Messages

### 4.1 PENDING → DUE

```json
{
  "stepInstanceId": "550e8400-e29b-41d4-a716-446655440001",
  "transitionType": "PENDING_TO_DUE",
  "triggeredAt": "2026-03-20T00:00:05Z",
  "correlationid": "sched-PENDING_TO_DUE-550e8400"
}
```

**Kafka Key:** `660e8400-e29b-41d4-a716-446655440001` (protocolInstanceId)

### 4.2 DUE → OVERDUE

```json
{
  "stepInstanceId": "770e8400-e29b-41d4-a716-446655440002",
  "transitionType": "DUE_TO_OVERDUE",
  "triggeredAt": "2026-03-25T00:00:05Z",
  "correlationid": "sched-DUE_TO_OVERDUE-770e8400"
}
```

**Kafka Key:** `660e8400-e29b-41d4-a716-446655440001` (protocolInstanceId)

### 4.3 OVERDUE → MISSED

```json
{
  "stepInstanceId": "880e8400-e29b-41d4-a716-446655440003",
  "transitionType": "OVERDUE_TO_MISSED",
  "triggeredAt": "2026-04-01T00:00:05Z",
  "correlationid": "sched-OVERDUE_TO_MISSED-880e8400"
}
```

**Kafka Key:** `660e8400-e29b-41d4-a716-446655440001` (protocolInstanceId)

---

## 5. Compliance Service Consumer Contract

The Compliance Service consumes from `cce.scheduler.triggers` with these expectations:

| # | Expectation | Description |
|---|-------------|-------------|
| 1 | `stepInstanceId` references a valid step | Compliance Service validates the step exists; ignores unknown IDs |
| 2 | `transitionType` matches current state | If the step has already been transitioned (e.g., completed by an event), the message is a no-op |
| 3 | Idempotent processing | Duplicate messages for the same step+transition produce no side effects |
| 4 | Kafka key = `protocolInstanceId` | Ensures per-protocol ordering within a partition |
| 5 | `correlationid` for tracing | Propagated to MDC for structured logging |
| 6 | `requiredBehavior` determines terminal state | On `OVERDUE_TO_MISSED`: `must` → `MISSED` + deviation; `could` → `SKIPPED` (no deviation) |
| 7 | Intelligence action evaluation | On deviation creation (`DUE_TO_OVERDUE`, `OVERDUE_TO_MISSED`), the Compliance Service evaluates PlanDefinition intelligence actions and publishes `IntelligenceTriggerEvent` to `cce.intelligence.triggers` |

---

## 6. Ordering & Delivery Guarantees

| Guarantee | Mechanism |
|---|---|
| **At-least-once delivery** | Synchronous publish + idempotent producer |
| **Idempotency (producer)** | `enable.idempotence=true` prevents duplicate publishes on retry |
| **Idempotency (consumer)** | Compliance Service checks current step state before applying transition |
| **Ordering (per partition)** | Key = `protocolInstanceId` ensures all transitions for a protocol are ordered. The Scheduler’s hash-partitioning by `protocol_instance_id` guarantees a given protocol is always scanned by the same Scheduler instance, preserving transition ordering even with multiple concurrent instances. |
| **Durability** | `acks=all` waits for all ISR replicas |

---

## 7. Error Handling

| Failure | Scheduler Behavior | Recovery |
|---|---|---|
| Kafka broker unavailable | Log error, skip this step, continue batch | Step picked up on next scan cycle |
| Publish timeout | Log error, increment failure metric | Same — next cycle retries |
| Serialization error | Should not occur (fixed schema) | Log and skip |
| All publishes fail in a cycle | Metrics show 100% failure rate | Alert via `cce.scheduler.publish.failure` metric |

The Scheduler does **not** publish to `cce.deadletter` — failed transitions are simply retried on the next scan cycle since the step's date threshold is still crossed.
