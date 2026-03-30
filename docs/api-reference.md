# CCE Scheduler Service — API Reference

The Scheduler Service is a **headless background service** with no business REST API endpoints. The only HTTP endpoints are Spring Boot Actuator management endpoints used for health checks, monitoring, and metrics collection.

---

## 1. Management Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/actuator/health` | Aggregate health status (includes leader election, database, Kafka) |
| GET | `/actuator/health/liveness` | Kubernetes liveness probe |
| GET | `/actuator/health/readiness` | Kubernetes readiness probe |
| GET | `/actuator/info` | Application info (version, build time) |
| GET | `/actuator/prometheus` | Prometheus-format metrics scrape endpoint |
| GET | `/actuator/metrics` | Available metrics listing |
| GET | `/actuator/metrics/{metricName}` | Specific metric detail |

---

## 2. Health Check Detail

### `GET /actuator/health`

Returns aggregate health status including custom `leaderElection` indicator.

**Response: `200 OK` (Healthy)**

```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP",
      "details": {
        "database": "PostgreSQL",
        "validationQuery": "isValid()"
      }
    },
    "kafka": {
      "status": "UP",
      "details": {
        "bootstrap.servers": "localhost:9092"
      }
    },
    "leaderElection": {
      "status": "UP",
      "details": {
        "leader": true,
        "partitionIndex": 0,
        "totalPartitions": 3,
        "leaderId": "scheduler-instance-1",
        "lastHeartbeat": "2026-03-25T10:00:05Z",
        "leaseExpiresAt": "2026-03-25T10:00:35Z"
      }
    }
  }
}
```

**Standby instance response:**

```json
{
  "status": "UP",
  "components": {
    "leaderElection": {
      "status": "UP",
      "details": {
        "leader": false,
        "partitionIndex": null,
        "totalPartitions": 3,
        "leaderId": null,
        "lastHeartbeat": null,
        "leaseExpiresAt": null
      }
    }
  }
}
```

> **Note:** `leaderElection.status` is `UP` as long as the election mechanism is functional — regardless of whether this instance is a partition leader. The `leader` field indicates the actual role, and `partitionIndex` shows which partition this instance owns (`null` for standby).

---

## 3. Prometheus Metrics

### `GET /actuator/prometheus`

Exposes all Micrometer metrics in Prometheus scrape format. Key scheduler-specific metrics:

```
# HELP cce_scheduler_scan_duration_seconds Time spent per scan cycle
# TYPE cce_scheduler_scan_duration_seconds summary
cce_scheduler_scan_duration_seconds_count{partition="0"} 120.0
cce_scheduler_scan_duration_seconds_sum{partition="0"} 3.456

# HELP cce_scheduler_scan_steps_total Steps found per transition type
# TYPE cce_scheduler_scan_steps_total counter
cce_scheduler_scan_steps_total{transition_type="PENDING_TO_DUE",partition="0"} 45.0
cce_scheduler_scan_steps_total{transition_type="DUE_TO_OVERDUE",partition="0"} 12.0
cce_scheduler_scan_steps_total{transition_type="OVERDUE_TO_MISSED",partition="0"} 3.0

# HELP cce_scheduler_publish_success_total Successful Kafka publishes
# TYPE cce_scheduler_publish_success_total counter
cce_scheduler_publish_success_total{transition_type="PENDING_TO_DUE",partition="0"} 45.0

# HELP cce_scheduler_leader_status Current leader status
# TYPE cce_scheduler_leader_status gauge
cce_scheduler_leader_status{partition="0"} 1.0

# HELP cce_scheduler_cycle_count_total Total scan cycles executed
# TYPE cce_scheduler_cycle_count_total counter
cce_scheduler_cycle_count_total{partition="0"} 120.0
```

---

## 4. No Business REST APIs

The Scheduler Service does not expose any business-facing REST endpoints. All coordination with the Compliance Service is via Kafka (`cce.scheduler.triggers` topic). There are no endpoints for:
- Triggering manual scans
- Viewing pending transitions
- Modifying scan configuration at runtime
- Pausing/resuming the scheduler

All operational control is via configuration properties (env vars) and redeployment.
