package org.openphc.cce.scheduler.engine;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.leader.LeaderElection;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchedulerLoopTest {

    @Mock
    private LeaderElection leaderElection;

    @Mock
    private DueStepScanner dueStepScanner;

    @Mock
    private TransitionPublisher transitionPublisher;

    @Mock
    private PartitionCursorService partitionCursor;

    private SchedulerProperties properties;
    private SimpleMeterRegistry meterRegistry;
    private ObservabilityConfig metrics;
    private SchedulerLoop schedulerLoop;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        meterRegistry = new SimpleMeterRegistry();
        metrics = new ObservabilityConfig(meterRegistry);
        schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                transitionPublisher, properties, meterRegistry, metrics, partitionCursor);
    }

    @Test
    void executeCycle_standby_skipsProcessing() {
        when(leaderElection.isLeader()).thenReturn(false);

        schedulerLoop.executeCycle();

        verify(dueStepScanner, never()).scan();
        verify(transitionPublisher, never()).publishAll(anyList());
    }

    @Test
    void executeCycle_leader_scansAndPublishes() {
        List<DueStep> steps = List.of(buildDueStep());
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(steps);
        when(transitionPublisher.publishAll(steps)).thenReturn(1);

        schedulerLoop.executeCycle();

        verify(dueStepScanner).scan();
        verify(transitionPublisher).publishAll(steps);
    }

    @Test
    void executeCycle_emptyBatch_stillRecordsCycleMetric() {
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList())).thenReturn(0);

        schedulerLoop.executeCycle();

        assertThat(meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count())
                .isEqualTo(1.0);
    }

    @Test
    void executeCycle_scannerThrows_doesNotTerminate() {
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenThrow(new RuntimeException("DB error"));

        schedulerLoop.executeCycle(); // should not throw

        assertThat(meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count())
                .isEqualTo(1.0);
    }

    @Test
    void executeCycle_recordsScanStepsCount() {
        List<DueStep> steps = List.of(buildDueStep(), buildDueStep(), buildDueStep());
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(steps);
        when(transitionPublisher.publishAll(steps)).thenReturn(3);

        schedulerLoop.executeCycle();

        assertThat(meterRegistry.counter("cce.scheduler.scan.steps",
                "transition_type", "all", "partition", "0").count()).isEqualTo(3.0);
    }

    @Test
    void executeCycle_allPublished_advancesCursorToLastEmitted() {
        OffsetDateTime earlier = OffsetDateTime.now(ZoneOffset.UTC).minusHours(2);
        OffsetDateTime latest = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        DueStep first = buildDueStepAt(earlier);
        DueStep last = buildDueStepAt(latest);
        List<DueStep> steps = List.of(first, last); // scanner returns (threshold, id) order
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(steps);
        when(transitionPublisher.publishAll(steps)).thenReturn(2);

        schedulerLoop.executeCycle();

        verify(partitionCursor).advanceWatermark(
                PartitionCursorService.SINGLE_PARTITION, latest, last.stepInstanceId());
    }

    @Test
    void executeCycle_partialPublishFailure_doesNotAdvanceCursor() {
        List<DueStep> steps = List.of(buildDueStep(), buildDueStep());
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(steps);
        when(transitionPublisher.publishAll(steps)).thenReturn(1); // one failed

        schedulerLoop.executeCycle();

        verify(partitionCursor, never()).advanceWatermark(anyInt(), any(), any());
    }

    @Test
    void executeCycle_emptyBatch_doesNotAdvanceCursor() {
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList())).thenReturn(0);

        schedulerLoop.executeCycle();

        verify(partitionCursor, never()).advanceWatermark(anyInt(), any(), any());
    }

    @Test
    void executeCycle_watermarkDisabled_doesNotAdvanceCursor() {
        properties.setWatermarkEnabled(false);
        List<DueStep> steps = List.of(buildDueStep());
        when(leaderElection.isLeader()).thenReturn(true);
        when(dueStepScanner.scan()).thenReturn(steps);
        when(transitionPublisher.publishAll(steps)).thenReturn(1);

        schedulerLoop.executeCycle();

        verify(partitionCursor, never()).advanceWatermark(anyInt(), any(), any());
    }

    private DueStep buildDueStep() {
        return buildDueStepAt(OffsetDateTime.now(ZoneOffset.UTC));
    }

    private DueStep buildDueStepAt(OffsetDateTime thresholdDate) {
        return new DueStep(
                UUID.randomUUID(),
                UUID.randomUUID(),
                TransitionType.PENDING_TO_DUE,
                thresholdDate,
                JsonNodeFactory.instance.objectNode()
        );
    }
}
