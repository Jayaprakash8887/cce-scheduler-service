package org.openphc.cce.scheduler.engine;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.StepInstance;
import org.openphc.cce.scheduler.domain.model.enums.StepState;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.domain.repository.StepInstanceRepository;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DueStepScannerTest {

    @Mock
    private StepInstanceRepository stepInstanceRepository;

    private SchedulerProperties properties;
    private DueStepScanner scanner;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setBatchSize(100);
        scanner = new DueStepScanner(stepInstanceRepository, properties);
    }

    @Test
    void scan_emptyResult_returnsEmptyList() {
        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Collections.emptyList());

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).isEmpty();
    }

    @Test
    void scan_pendingStep_returnsPendingToDue() {
        StepInstance step = buildStepInstance(StepState.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        DueStep dueStep = result.get(0);
        assertThat(dueStep.transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
        assertThat(dueStep.thresholdDate()).isEqualTo(step.getDueDate());
        assertThat(dueStep.stepInstanceId()).isEqualTo(step.getId());
        assertThat(dueStep.protocolInstanceId()).isEqualTo(step.getProtocolInstanceId());
    }

    @Test
    void scan_dueStep_returnsDueToOverdue() {
        OffsetDateTime overdueDate = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(30);
        StepInstance step = buildStepInstance(StepState.DUE, null, overdueDate, null);

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.DUE_TO_OVERDUE);
        assertThat(result.get(0).thresholdDate()).isEqualTo(overdueDate);
    }

    @Test
    void scan_overdueStep_returnsOverdueToMissed() {
        OffsetDateTime missedDate = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(10);
        StepInstance step = buildStepInstance(StepState.OVERDUE, null, null, missedDate);

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
        assertThat(result.get(0).thresholdDate()).isEqualTo(missedDate);
    }

    @Test
    void scan_metadata_containsActionIdAndRepeatIndex() {
        StepInstance step = buildStepInstance(StepState.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        JsonNode metadata = result.get(0).metadata();
        assertThat(metadata.get("actionId").asText()).isEqualTo("action-1");
        assertThat(metadata.get("repeatIndex").asInt()).isEqualTo(0);
        assertThat(metadata.get("currentState").asText()).isEqualTo("PENDING");
        assertThat(metadata.get("transitionType").asText()).isEqualTo("PENDING_TO_DUE");
    }

    @Test
    void scan_metadata_includesRequiredBehaviorWhenPresent() {
        StepInstance step = buildStepInstance(StepState.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);
        setField(step, "requiredBehavior", "MUST");

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        JsonNode metadata = result.get(0).metadata();
        assertThat(metadata.has("requiredBehavior")).isTrue();
        assertThat(metadata.get("requiredBehavior").asText()).isEqualTo("MUST");
    }

    @Test
    void scan_metadata_omitsRequiredBehaviorWhenNull() {
        StepInstance step = buildStepInstance(StepState.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(step));

        List<DueStep> result = scanner.scan(0, 1);

        JsonNode metadata = result.get(0).metadata();
        assertThat(metadata.has("requiredBehavior")).isFalse();
    }

    @Test
    void scan_multipleSteps_returnsAll() {
        StepInstance pending = buildStepInstance(StepState.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(2), null, null);
        StepInstance due = buildStepInstance(StepState.DUE, null,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null);
        StepInstance overdue = buildStepInstance(StepState.OVERDUE, null, null,
                OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(30));

        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(List.of(pending, due, overdue));

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
        assertThat(result.get(1).transitionType()).isEqualTo(TransitionType.DUE_TO_OVERDUE);
        assertThat(result.get(2).transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
    }

    @Test
    void scan_passesPartitionParametersToRepository() {
        when(stepInstanceRepository.findDueSteps(any(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Collections.emptyList());

        scanner.scan(3, 8);

        org.mockito.Mockito.verify(stepInstanceRepository)
                .findDueSteps(any(OffsetDateTime.class),
                        org.mockito.ArgumentMatchers.eq(3),
                        org.mockito.ArgumentMatchers.eq(8),
                        org.mockito.ArgumentMatchers.eq(100));
    }

    private StepInstance buildStepInstance(StepState state,
                                           OffsetDateTime dueDate,
                                           OffsetDateTime overdueDate,
                                           OffsetDateTime missedDate) {
        StepInstance step = new StepInstance();
        setField(step, "id", UUID.randomUUID());
        setField(step, "protocolInstanceId", UUID.randomUUID());
        setField(step, "actionId", "action-1");
        setField(step, "repeatIndex", 0);
        setField(step, "state", state);
        setField(step, "dueDate", dueDate);
        setField(step, "overdueDate", overdueDate);
        setField(step, "missedDate", missedDate);
        setField(step, "createdAt", OffsetDateTime.now(ZoneOffset.UTC));
        setField(step, "updatedAt", OffsetDateTime.now(ZoneOffset.UTC));
        return step;
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
    }
}
