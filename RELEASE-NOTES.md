# Release Notes — CCE Scheduler Service v1.0.0

**Release Date:** 2026-05-06  
**Branch:** `release-1.0.0`  
**Artifact:** `openphc/cce-scheduler-service:1.0.0`

---

## Overview

Initial production release of the CCE Scheduler Service — a headless background service that drives time-based step state transitions in the CCE platform by polling `step_instance` and publishing transition triggers to Kafka.

---

## Features

### Core Engine
- **Single-leader architecture** — one active leader scans the entire `step_instance` table; all other replicas stand by for fast failover
- **Configurable scan loop** — `@Scheduled` fixedDelay with tunable interval and batch size
- **Due step scanner** — keyset/seek cursor over `(threshold, id)` prevents re-scanning and re-publishing crossings already emitted
- **Transition publisher** — idempotent Kafka producer, async batched publish (fire all sends per cycle, then await the batch), with correlation ID tracking

### Leader Election
- PostgreSQL advisory lock-based single-leader election (`pg_try_advisory_lock`)
- Dedicated JDBC connection (outside connection pool) for lock stability
- Lease-based heartbeat with configurable TTL, upserted to `scheduler_lease` for observability
- Standbys report `UP` (healthy-but-not-leader) via the health indicator so all replicas stay ready for fast failover

### Kafka Integration
- Idempotent producer (`enable.idempotence=true`, `acks=all`)
- Async batched publishing — sends are fired without blocking, then the whole cycle's batch is awaited
- Structured trigger events on `cce.scheduler.triggers` topic
- `batch.size`/`linger.ms` tuned to coalesce a scan cycle's sends
- Correlation ID format: `sched-{transitionType}-{stepIdPrefix}`

### Observability
- Micrometer + Prometheus metrics (no partition tags — single scan domain)
- Cached metric instances (ConcurrentHashMap) to avoid recreation overhead
- Leader status gauge
- Scan duration timer, step counter, publish success/failure counters
- Spring Boot Actuator health probes (liveness + readiness)

### Data Management
- Flyway-managed schema migrations (`V1__create_scheduler_lease.sql`, `V2__scheduler_scan_cursor.sql`)
- `scheduler_lease` — single row keyed by `id = 0` (leader heartbeat)
- `scheduler_scan_cursor` — single row keyed by `id = 0`, the `(threshold, id)` watermark
- HikariCP connection pool sized for the single-leader model (min 2)

---

## Configuration

All settings configurable via environment variables. See [Deployment Guide](docs/deployment-guide.md) for the full reference.

Key defaults:
- Scan interval: 10000ms
- Batch size: 1000
- Lease duration: 30s
- Advisory lock key: 100001 (single-leader election)

---

## Infrastructure Requirements

| Component    | Minimum Version |
|------------- |-----------------|
| Java         | 21 LTS          |
| PostgreSQL   | 16              |
| Apache Kafka | 3.6             |

---

## Docker Image

- **Base:** `eclipse-temurin:21-jre-alpine`
- **JVM:** ZGC, 75% max RAM
- **User:** Non-root (`scheduler`)
- **Health check:** Liveness probe via wget

```bash
docker build -t openphc/cce-scheduler-service:1.0.0 .
```

---

## Database Migrations

| Version | Description              |
|---------|--------------------------|
| V1      | Create `scheduler_lease` table (single row, `id = 0`) |
| V2      | Create `scheduler_scan_cursor` table (single row, `id = 0`) and partial scan indexes on `step_instance` |

---

## Known Limitations

- No REST API — communication is exclusively via Kafka and database polling
- Single active leader — the whole scan/publish workload runs on one instance at a time; standbys add failover capacity, not throughput
- Requires shared `ccedb` database with Compliance Service
- Advisory locks are session-scoped — connection loss triggers leadership loss

---

## Upgrade Path

This is the initial release. No migration from prior versions required.

---

## Contributors

- OpenPHC Engineering Team
