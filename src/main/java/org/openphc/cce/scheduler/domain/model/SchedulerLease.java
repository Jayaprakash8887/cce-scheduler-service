package org.openphc.cce.scheduler.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "scheduler_lease")
@Getter
@Setter
@NoArgsConstructor
public class SchedulerLease {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "partition_index", nullable = false, unique = true)
    private int partitionIndex;

    @Column(name = "leader_id")
    private String leaderId;

    @Column(name = "last_heartbeat")
    private OffsetDateTime lastHeartbeat;

    @Column(name = "lease_expires_at")
    private OffsetDateTime leaseExpiresAt;
}
