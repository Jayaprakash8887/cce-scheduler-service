package org.openphc.cce.scheduler.engine;

import com.fasterxml.jackson.databind.JsonNode;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;

import java.time.OffsetDateTime;
import java.util.UUID;

public record DueStep(
        UUID stepInstanceId,
        UUID protocolInstanceId,
        TransitionType transitionType,
        OffsetDateTime thresholdDate,
        JsonNode metadata
) {
}
