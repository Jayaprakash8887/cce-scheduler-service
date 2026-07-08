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
import static org.mockito.Mockito.times;
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
        properties.setTotalPartitions(4);
        meterRegistry = new SimpleMeterRegistry();
        metrics = new ObservabilityConfig(meterRegistry);
        schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                transitionPublisher, properties, meterRegistry, metrics, partitionCursor);
    }

    @Test
    void executeCycle_standby_skipsProcessing() {
        when(leaderElection.getOwnedPartitions()).thenReturn(Collections.emptyList());

        schedulerLoop.executeCycle();

        verify(dueStepScanner, never()).scan(anyInt(), anyInt());
        verify(transitionPublisher, never()).publishAll(anyList(), anyInt());
    }

    @Test
    void executeCycle_singlePartition_scansAndPublishes() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        List<DueStep> steps = List.of(buildDueStep());
        when(dueStepScanner.scan(0, 4)).thenReturn(steps);
        when(transitionPublisher.publishAll(steps, 0)).thenReturn(1);

        schedulerLoop.executeCycle();

        verify(dueStepScanner).scan(0, 4);
        verify(transitionPublisher).publishAll(steps, 0);
    }

    @Test
    void executeCycle_multiplePartitions_processesEach() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1, 2));
        when(dueStepScanner.scan(anyInt(), anyInt())).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList(), anyInt())).thenReturn(0);

        schedulerLoop.executeCycle();

        verify(dueStepScanner).scan(0, 4);
        verify(dueStepScanner).scan(1, 4);
        verify(dueStepScanner).scan(2, 4);
        verify(transitionPublisher, times(3)).publishAll(anyList(), anyInt());
    }

    @Test
    void executeCycle_emptyBatch_stillRecordsMetrics() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList(), anyInt())).thenReturn(0);

        schedulerLoop.executeCycle();

        double cycleCount = meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count();
        assertThat(cycleCount).isEqualTo(1.0);
    }

    @Test
    void executeCycle_scannerThrows_doesNotTerminate() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1));
        when(dueStepScanner.scan(0, 4)).thenThrow(new RuntimeException("DB error"));
        when(dueStepScanner.scan(1, 4)).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList(), anyInt())).thenReturn(0);

        // Should not throw
        schedulerLoop.executeCycle();

        // Second partition still processed despite first one failing
        verify(dueStepScanner).scan(1, 4);
    }

    @Test
    void executeCycle_recordsScanDuration() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList(), anyInt())).thenReturn(0);

        schedulerLoop.executeCycle();

        long timerCount = meterRegistry.timer("cce.scheduler.scan.duration", "partition", "0").count();
        assertThat(timerCount).isEqualTo(1);
    }

    @Test
    void executeCycle_recordsScanStepsCount() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        List<DueStep> steps = List.of(buildDueStep(), buildDueStep(), buildDueStep());
        when(dueStepScanner.scan(0, 4)).thenReturn(steps);
        when(transitionPublisher.publishAll(steps, 0)).thenReturn(3);

        schedulerLoop.executeCycle();

        double stepsCount = meterRegistry.counter("cce.scheduler.scan.steps",
                "transition_type", "all", "partition", "0").count();
        assertThat(stepsCount).isEqualTo(3.0);
    }

    @Test
    void executeCycle_allPublished_advancesCursorToLastEmitted() {
        OffsetDateTime earlier = OffsetDateTime.now(ZoneOffset.UTC).minusHours(2);
        OffsetDateTime latest = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        DueStep first = buildDueStepAt(earlier);
        DueStep last = buildDueStepAt(latest);
        // Scanner returns rows in (threshold, id) order; the loop advances to the LAST one.
        List<DueStep> steps = List.of(first, last);
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(steps);
        when(transitionPublisher.publishAll(steps, 0)).thenReturn(2);

        schedulerLoop.executeCycle();

        verify(partitionCursor).advanceWatermark(0, latest, last.stepInstanceId());
    }

    @Test
    void executeCycle_partialPublishFailure_doesNotAdvanceWatermark() {
        List<DueStep> steps = List.of(buildDueStep(), buildDueStep());
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(steps);
        when(transitionPublisher.publishAll(steps, 0)).thenReturn(1); // one failed

        schedulerLoop.executeCycle();

        verify(partitionCursor, never()).advanceWatermark(anyInt(), any(), any());
    }

    @Test
    void executeCycle_emptyBatch_doesNotAdvanceWatermark() {
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(Collections.emptyList());
        when(transitionPublisher.publishAll(anyList(), anyInt())).thenReturn(0);

        schedulerLoop.executeCycle();

        verify(partitionCursor, never()).advanceWatermark(anyInt(), any(), any());
    }

    @Test
    void executeCycle_watermarkDisabled_doesNotAdvanceWatermark() {
        properties.setWatermarkEnabled(false);
        List<DueStep> steps = List.of(buildDueStep());
        when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
        when(dueStepScanner.scan(0, 4)).thenReturn(steps);
        when(transitionPublisher.publishAll(steps, 0)).thenReturn(1);

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
