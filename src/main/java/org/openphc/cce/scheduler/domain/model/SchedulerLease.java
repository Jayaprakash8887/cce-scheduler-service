package org.openphc.cce.scheduler.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * Leader heartbeat — a single row (id = 0) upserted by the current leader. Write-only
 * observability of who leads and when they last heartbeat; leadership itself is the
 * advisory lock, and health reads in-memory state, not this row.
 */
@Entity
@Table(name = "scheduler_lease")
@Getter
@Setter
@NoArgsConstructor
public class SchedulerLease {

    @Id
    @Column(name = "id")
    private int id;

    @Column(name = "leader_id")
    private String leaderId;

    @Column(name = "last_heartbeat")
    private OffsetDateTime lastHeartbeat;

    @Column(name = "lease_expires_at")
    private OffsetDateTime leaseExpiresAt;
}
