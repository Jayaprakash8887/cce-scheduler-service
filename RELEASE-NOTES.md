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
- **Partitioned multi-leader architecture** — greedy partition acquisition via `pg_advisory_lock`
- **Configurable scan loop** — `@Scheduled` fixedDelay with tunable interval and batch size
- **Due step scanner** — partition-filtered queries with deterministic MD5-based hashing
- **Transition publisher** — idempotent Kafka producer with correlation ID tracking

### Leader Election
- PostgreSQL advisory lock-based leader election per partition
- Dedicated JDBC connection (outside connection pool) for lock stability
- Lease-based heartbeat with configurable TTL
- Thread-safe `LeaderState` record for atomic state reads
- Jittered retry on lock acquisition failure

### Kafka Integration
- Idempotent producer (`enable.idempotence=true`, `acks=all`)
- Structured trigger events on `cce.scheduler.triggers` topic
- Configurable timeouts: request (5s), delivery (15s), linger (50ms)
- Correlation ID format: `sched-{transitionType}-{stepIdPrefix}-{timestamp}`

### Observability
- Micrometer + Prometheus metrics with partition tags
- Cached metric instances (ConcurrentHashMap) to avoid recreation overhead
- Leader status gauge per partition
- Scan duration timer, step counter, publish success/failure counters
- Spring Boot Actuator health probes (liveness + readiness)

### Data Management
- Flyway-managed schema migrations
- `scheduler_lease` table with partition index uniqueness constraint
- HikariCP connection pool with leak detection (15s threshold)

---

## Configuration

All settings configurable via environment variables. See [Deployment Guide](docs/deployment-guide.md) for the full reference.

Key defaults:
- Scan interval: 5000ms
- Batch size: 100
- Lease duration: 30s
- Total partitions: 1 (single-leader mode)

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
| V1      | Create `scheduler_lease` table with default partition row |

---

## Known Limitations

- No REST API — communication is exclusively via Kafka and database polling
- Partition rebalancing is greedy (not cooperative) — works best when replicas ≤ partitions
- Requires shared `ccedb` database with Compliance Service
- Advisory locks are session-scoped — connection loss triggers leadership loss

---

## Upgrade Path

This is the initial release. No migration from prior versions required.

---

## Contributors

- OpenPHC Engineering Team
