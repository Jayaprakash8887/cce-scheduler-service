package org.openphc.cce.scheduler.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.StepInstance;
import org.openphc.cce.scheduler.domain.model.enums.StepState;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.domain.repository.StepInstanceRepository;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Component
@Slf4j
public class DueStepScanner {

    private final StepInstanceRepository stepInstanceRepository;
    private final SchedulerProperties properties;
    private final PartitionCursorService partitionCursor;

    public DueStepScanner(StepInstanceRepository stepInstanceRepository,
                          SchedulerProperties properties,
                          PartitionCursorService partitionCursor) {
        this.stepInstanceRepository = stepInstanceRepository;
        this.properties = properties;
        this.partitionCursor = partitionCursor;
    }

    public List<DueStep> scan(int partitionIndex, int totalPartitions) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime watermark = properties.isWatermarkEnabled()
                ? partitionCursor.readWatermark(partitionIndex)
                : PartitionCursorService.BEGINNING;

        List<StepInstance> dueSteps = stepInstanceRepository.findDueSteps(
                now, watermark, partitionIndex, totalPartitions, properties.getBatchSize());

        log.debug("Scanned partition {}/{} — watermark={}, found {} due steps",
                partitionIndex, totalPartitions, watermark, dueSteps.size());

        return dueSteps.stream()
                .map(this::toDueStep)
                .toList();
    }

    private DueStep toDueStep(StepInstance step) {
        TransitionType transitionType = TransitionType.fromState(step.getState());
        OffsetDateTime thresholdDate = resolveThresholdDate(step);
        JsonNode metadata = buildMetadata(step, transitionType);

        return new DueStep(
                step.getId(),
                step.getProtocolInstanceId(),
                transitionType,
                thresholdDate,
                metadata
        );
    }

    private OffsetDateTime resolveThresholdDate(StepInstance step) {
        return switch (step.getState()) {
            case PENDING -> step.getDueDate();
            case DUE -> step.getOverdueDate();
            case OVERDUE -> step.getMissedDate();
            default -> throw new IllegalStateException(
                    "Unexpected state in due step scan: " + step.getState());
        };
    }

    private JsonNode buildMetadata(StepInstance step, TransitionType transitionType) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("actionId", step.getActionId());
        node.put("repeatIndex", step.getRepeatIndex());
        node.put("currentState", step.getState().name());
        node.put("transitionType", transitionType.name());
        if (step.getRequiredBehavior() != null) {
            node.put("requiredBehavior", step.getRequiredBehavior());
        }
        return node;
    }
}
