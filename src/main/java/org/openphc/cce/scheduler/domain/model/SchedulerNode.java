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
 * Heartbeat record for a live scheduler pod. Every pod registers here each
 * leader-election cycle — including standbys that currently own no partitions —
 * so the cluster size can be counted and each pod can claim only its fair share.
 */
@Entity
@Table(name = "scheduler_node")
@Getter
@Setter
@NoArgsConstructor
public class SchedulerNode {

    @Id
    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "last_heartbeat", nullable = false)
    private OffsetDateTime lastHeartbeat;

    @Column(name = "owned_count", nullable = false)
    private int ownedCount;
}
