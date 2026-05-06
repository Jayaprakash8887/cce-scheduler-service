package org.openphc.cce.scheduler.kafka;

import org.openphc.cce.scheduler.domain.model.enums.TransitionType;

import java.time.OffsetDateTime;
import java.util.UUID;

public record SchedulerTriggerMessage(
        UUID stepInstanceId,
        TransitionType transitionType,
        OffsetDateTime triggeredAt,
        String correlationId
) {
}
