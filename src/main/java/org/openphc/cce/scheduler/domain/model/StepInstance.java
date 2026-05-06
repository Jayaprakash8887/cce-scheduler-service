package org.openphc.cce.scheduler.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;
import org.openphc.cce.scheduler.domain.model.enums.StepState;

import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "step_instance")
@Immutable
@Getter
@NoArgsConstructor
public class StepInstance {

    @Id
    private UUID id;

    @Column(name = "protocol_instance_id", nullable = false)
    private UUID protocolInstanceId;

    @Column(name = "action_id", nullable = false)
    private String actionId;

    @Column(name = "repeat_index", nullable = false)
    private int repeatIndex;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false)
    private StepState state;

    @Column(name = "due_date")
    private OffsetDateTime dueDate;

    @Column(name = "overdue_date")
    private OffsetDateTime overdueDate;

    @Column(name = "missed_date")
    private OffsetDateTime missedDate;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @Column(name = "required_behavior")
    private String requiredBehavior;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
}
