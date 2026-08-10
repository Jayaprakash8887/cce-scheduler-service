# CCE Scheduler Service

A headless background service within the CCE platform that drives **time-based step state transitions**. It polls the Compliance Service's `step_instance` table for steps that have crossed their time thresholds and publishes transition requests to Kafka.

> **No REST API endpoints** — communication with the Compliance Service is exclusively via Kafka. The only HTTP endpoints are Spring Boot Actuator management endpoints for health checks and metrics.

---

## Overview

The Scheduler detects when clinical protocol steps become **DUE** or **MISSED** based on configured date thresholds, and notifies the Compliance Service to perform the actual state transitions.

| Transition | Condition |
|---|---|
| `PENDING → DUE` | `due_date ≤ now` |
| `DUE → MISSED` | `missed_date ≤ now` |

### Key Characteristics

- **Scan-publish loop** — runs on a configurable `fixedDelay` cadence (default: 5 seconds)
- **Leader election** — uses PostgreSQL advisory locks for active-standby HA (deploy 2+ instances)
- **Read-only database access** — never writes to `step_instance`; the Compliance Service handles actual transitions
- **Idempotent** — duplicate Kafka messages are safely ignored by the Compliance Service
- **Single Kafka topic** — produces to `cce.scheduler.triggers`, consumes from none

---

## Technology Stack

| Concern | Technology |
|---|---|
| Language | Java 21 (LTS) |
| Framework | Spring Boot 3.4.x |
| Build | Gradle 8.x |
| Database | PostgreSQL 16+ (shared `ccedb` DB) |
| Messaging | Apache Kafka 3.7+ (KRaft mode) |
| DB Access | Spring Data JPA + Hibernate |
| Migrations | Flyway |
| Observability | Micrometer + Prometheus |
| Testing | JUnit 5, Testcontainers |

---

## Quick Start

### Prerequisites

- Java JDK 21
- Docker & Docker Compose (for PostgreSQL + Kafka)
- The **CCE Collector Service** infrastructure running (shared database + Kafka)

### Build & Run

```bash
# Clone and build
git clone <repository-url>
cd cce-scheduler-service
./gradlew build

# Start shared infrastructure (PostgreSQL + Kafka — deployed by CCE Collector Service)
cd /path/to/cce-collector-service
docker compose up -d

# Run the Scheduler
cd /path/to/cce-scheduler-service
./gradlew bootRun

# Verify health
curl localhost:8083/actuator/health
```

> The Scheduler connects to the same `ccedb` database as all other CCE services. The `step_instance` table must exist (created by Compliance Service migrations) before the Scheduler can scan.

---

## Configuration

Key environment variables (all have sensible defaults for local development):

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8083` | HTTP port (actuator only) |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5433` | PostgreSQL port |
| `DB_NAME` | `ccedb` | Shared database name |
| `DB_USERNAME` | `cce_user` | Database username |
| `DB_PASSWORD` | `cce_pass` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `SCHEDULER_SCAN_INTERVAL` | `5000` | Scan interval (ms) |
| `SCHEDULER_BATCH_SIZE` | `100` | Max steps per scan cycle |

See [docs/developer-setup.md](docs/developer-setup.md) for the full configuration reference.

---

## Architecture

```
┌─────────────────────────────────────┐
│       CCE Scheduler Service         │
│                                     │
│  Leader Election (pg_advisory_lock) │
│  Scheduler Loop (@Scheduled)        │
│  Due Step Scanner ──► PostgreSQL    │  (read-only)
│  Transition Publisher ──► Kafka     │  (cce.scheduler.triggers)
└─────────────────────────────────────┘
         │                     │
         ▼                     ▼
   ┌──────────┐        ┌────────────┐
   │PostgreSQL│        │Apache Kafka│
   │(shared)  │        │            │
   └──────────┘        └─────┬──────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │CCE Compliance Service │
                  │(performs transitions) │
                  └───────────────────────┘
```

### Core Loop

1. **Leader check** — only the leader instance processes; standby instances skip
2. **Scan** — query `step_instance` for rows where time thresholds are met
3. **Publish** — send a `SchedulerTriggerMessage` to Kafka for each due step
4. **Heartbeat** — update `scheduler_lease` table
5. **Wait** — `fixedDelay` then repeat

### High Availability

- Deploy 2+ instances for active-standby failover
- Leader election via PostgreSQL advisory locks
- Failover time ≤ 5 seconds (configurable via `SCHEDULER_LEADER_RETRY`)

---

## Project Structure

```
src/main/java/org/openphc/cce/scheduler/
├── SchedulerServiceApplication.java     # @SpringBootApplication + @EnableScheduling
├── config/                              # Configuration properties, Kafka, JPA, scheduling
├── domain/model/                        # StepInstance (read-only), SchedulerLease, StepState enum
├── domain/repository/                   # JPA repositories
├── engine/                              # SchedulerLoop, DueStepScanner, TransitionPublisher
├── health/                              # LeaderHealthIndicator
├── kafka/                               # SchedulerTriggerMessage, SchedulerTriggerProducer
└── leader/                              # LeaderElection (pg_advisory_lock)
```

---

## Testing

```bash
# Unit tests
./gradlew test

# Integration tests (requires Docker for Testcontainers)
./gradlew integrationTest

# All tests with coverage
./gradlew build jacocoTestReport
```

Integration tests use **Testcontainers** with real PostgreSQL and Kafka instances — no mocks.

---

## Docker

```bash
# Build image
docker build -t cce-scheduler-service .

# Run
docker run -p 8083:8083 \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5433 \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  cce-scheduler-service
```

---

## Observability

| Endpoint | Description |
|---|---|
| `GET /actuator/health` | Aggregate health (DB, Kafka, leader election) |
| `GET /actuator/health/liveness` | Kubernetes liveness probe |
| `GET /actuator/health/readiness` | Kubernetes readiness probe |
| `GET /actuator/prometheus` | Prometheus metrics scrape endpoint |

Key metrics: `cce.scheduler.scan.duration`, `cce.scheduler.scan.steps`, `cce.scheduler.publish.success`, `cce.scheduler.leader.status`, `cce.scheduler.cycle.count`.

---

## Documentation

| Document | Description |
|---|---|
| [Architecture Overview](docs/architecture-overview.md) | System context, component design, threading model, error handling |
| [API Reference](docs/api-reference.md) | Actuator endpoints, health checks, Prometheus metrics |
| [Data Dictionary](docs/data-dictionary.md) | Database tables, enums, Kafka message fields, configuration properties |
| [Kafka Events](docs/kafka-events.md) | Topic schema, sample payloads, producer configuration, delivery guarantees |
| [Flow Diagrams](docs/flow-diagrams.md) | Mermaid diagrams for scan loop, leader election, failover scenarios |
| [Developer Setup](docs/developer-setup.md) | Prerequisites, build commands, full configuration reference, Docker build |
