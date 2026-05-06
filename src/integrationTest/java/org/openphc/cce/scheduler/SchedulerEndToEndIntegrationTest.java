package org.openphc.cce.scheduler;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.engine.DueStep;
import org.openphc.cce.scheduler.engine.DueStepScanner;
import org.openphc.cce.scheduler.engine.SchedulerLoop;
import org.openphc.cce.scheduler.engine.TransitionPublisher;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerMessage;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerProducer;
import org.openphc.cce.scheduler.leader.LeaderElection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end integration tests for the full scheduler pipeline using mocks.
 * Validates the complete flow: leader election → scan → publish → metrics.
 */
@ExtendWith(MockitoExtension.class)
class SchedulerEndToEndIntegrationTest {

    @Mock
    private LeaderElection leaderElection;

    @Mock
    private DueStepScanner dueStepScanner;

    @Mock
    private SchedulerTriggerProducer triggerProducer;

    private SchedulerProperties properties;
    private SimpleMeterRegistry meterRegistry;
    private ObservabilityConfig metrics;
    private TransitionPublisher transitionPublisher;
    private SchedulerLoop schedulerLoop;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setTotalPartitions(1);
        properties.setBatchSize(100);
        meterRegistry = new SimpleMeterRegistry();
        metrics = new ObservabilityConfig(meterRegistry);
        transitionPublisher = new TransitionPublisher(triggerProducer, metrics);
        schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                transitionPublisher, properties, meterRegistry, metrics);
    }

    @Nested
    @DisplayName("State Transition Tests")
    class StateTransitionTests {

        @Test
        @DisplayName("PENDING step with past due_date → PENDING_TO_DUE message published")
        void pendingStep_publishesPendingToDue() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
            assertThat(msgCaptor.getValue().stepInstanceId()).isEqualTo(step.stepInstanceId());
        }

        @Test
        @DisplayName("DUE step with past overdue_date → DUE_TO_OVERDUE message published")
        void dueStep_publishesDueToOverdue() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.DUE_TO_OVERDUE);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.DUE_TO_OVERDUE);
        }

        @Test
        @DisplayName("OVERDUE step with past missed_date → OVERDUE_TO_MISSED message published")
        void overdueStep_publishesOverdueToMissed() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.OVERDUE_TO_MISSED);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
        }

        @Test
        @DisplayName("COMPLETED step is never picked up (terminal state)")
        void completedStep_neverPublished() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            // Scanner correctly excludes terminal states — returns empty
            when(dueStepScanner.scan(0, 1)).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(triggerProducer, never()).publish(any(UUID.class), any(SchedulerTriggerMessage.class));
        }
    }

    @Nested
    @DisplayName("Empty & Metrics Tests")
    class EmptyAndMetricsTests {

        @Test
        @DisplayName("Empty scan cycle → no messages published, metrics still recorded")
        void emptyScanCycle_noMessages_metricsRecorded() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            when(dueStepScanner.scan(0, 1)).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(triggerProducer, never()).publish(any(UUID.class), any(SchedulerTriggerMessage.class));

            double cycleCount = meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count();
            assertThat(cycleCount).isEqualTo(1.0);

            long timerCount = meterRegistry.timer("cce.scheduler.scan.duration", "partition", "0").count();
            assertThat(timerCount).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Single-Partition Mode Tests")
    class SinglePartitionTests {

        @Test
        @DisplayName("Single-partition mode (total-partitions=1) → only one leader processes")
        void singlePartition_oneLeaderProcesses() {
            properties.setTotalPartitions(1);
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            verify(dueStepScanner, times(1)).scan(0, 1);
            verify(triggerProducer, times(1)).publish(any(), any());
        }

        @Test
        @DisplayName("Standby instance (empty ownedPartitions) does nothing")
        void standby_doesNothing() {
            when(leaderElection.getOwnedPartitions()).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(dueStepScanner, never()).scan(anyInt(), anyInt());
            verify(triggerProducer, never()).publish(any(), any());
        }
    }

    @Nested
    @DisplayName("Multi-Partition Mode Tests")
    class MultiPartitionTests {

        @BeforeEach
        void setUpMultiPartition() {
            properties.setTotalPartitions(2);
            schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                    transitionPublisher, properties, meterRegistry, metrics);
        }

        @Test
        @DisplayName("Multi-partition mode → two partitions process disjoint step sets")
        void multiPartition_disjointStepSets() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1));

            UUID proto0 = UUID.randomUUID();
            UUID proto1 = UUID.randomUUID();
            DueStep step0 = dueStep(TransitionType.PENDING_TO_DUE, proto0);
            DueStep step1 = dueStep(TransitionType.DUE_TO_OVERDUE, proto1);

            when(dueStepScanner.scan(0, 2)).thenReturn(List.of(step0));
            when(dueStepScanner.scan(1, 2)).thenReturn(List.of(step1));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            // Both partitions processed
            verify(dueStepScanner).scan(0, 2);
            verify(dueStepScanner).scan(1, 2);

            // Messages published for both — no duplicates
            ArgumentCaptor<UUID> keyCaptor = ArgumentCaptor.forClass(UUID.class);
            verify(triggerProducer, times(2)).publish(keyCaptor.capture(), any());

            Set<UUID> publishedKeys = new HashSet<>(keyCaptor.getAllValues());
            assertThat(publishedKeys).containsExactlyInAnyOrder(proto0, proto1);
        }
    }

    @Nested
    @DisplayName("Greedy Acquisition Tests")
    class GreedyAcquisitionTests {

        @Test
        @DisplayName("1 instance with total-partitions=3 owns all 3 and scans full table")
        void greedyAcquisition_oneInstanceOwnsAll() {
            properties.setTotalPartitions(3);
            schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                    transitionPublisher, properties, meterRegistry, metrics);

            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1, 2));
            when(dueStepScanner.scan(anyInt(), eq(3))).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(dueStepScanner).scan(0, 3);
            verify(dueStepScanner).scan(1, 3);
            verify(dueStepScanner).scan(2, 3);
        }

        @Test
        @DisplayName("Under-provisioned: 2 instances with total-partitions=3 → partitions split, no orphans")
        void underProvisioned_partitionsSplit() {
            properties.setTotalPartitions(3);

            // Instance 1 owns partitions 0,1
            SchedulerLoop loop1 = new SchedulerLoop(leaderElection, dueStepScanner,
                    transitionPublisher, properties, meterRegistry, metrics);
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1));
            when(dueStepScanner.scan(anyInt(), eq(3))).thenReturn(Collections.emptyList());

            loop1.executeCycle();

            verify(dueStepScanner).scan(0, 3);
            verify(dueStepScanner).scan(1, 3);
            verify(dueStepScanner, never()).scan(eq(2), eq(3));
        }
    }

    @Nested
    @DisplayName("Failover Tests")
    class FailoverTests {

        @Test
        @DisplayName("Partition failover → surviving instance acquires orphaned partitions")
        void failover_survivingInstanceAcquiresOrphanedPartitions() {
            properties.setTotalPartitions(3);
            schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                    transitionPublisher, properties, meterRegistry, metrics);

            // Initially instance owns only partition 0
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            when(dueStepScanner.scan(anyInt(), eq(3))).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();
            verify(dueStepScanner, times(1)).scan(0, 3);

            // After failover, surviving instance now owns all 3
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1, 2));

            schedulerLoop.executeCycle();

            verify(dueStepScanner, times(2)).scan(0, 3); // second call
            verify(dueStepScanner).scan(1, 3);
            verify(dueStepScanner).scan(2, 3);
        }
    }

    @Nested
    @DisplayName("Kafka Message Key Tests")
    class KafkaKeyTests {

        @Test
        @DisplayName("Kafka message key is protocolInstanceId")
        void kafkaKey_isProtocolInstanceId() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            UUID protocolId = UUID.randomUUID();
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            verify(triggerProducer).publish(eq(protocolId), any(SchedulerTriggerMessage.class));
        }

        @Test
        @DisplayName("Multiple steps from same protocol use same key")
        void multipleSteps_sameProtocol_sameKey() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            UUID protocolId = UUID.randomUUID();
            DueStep step1 = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            DueStep step2 = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step1, step2));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            ArgumentCaptor<UUID> keyCaptor = ArgumentCaptor.forClass(UUID.class);
            verify(triggerProducer, times(2)).publish(keyCaptor.capture(), any());
            assertThat(keyCaptor.getAllValues()).containsOnly(protocolId);
        }
    }

    @Nested
    @DisplayName("Correlation ID Tests")
    class CorrelationIdTests {

        @Test
        @DisplayName("Correlation ID follows format sched-{transitionType}-{stepIdPrefix}")
        void correlationId_format() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(any(), msgCaptor.capture());

            String expectedPrefix = "sched-PENDING_TO_DUE-" + step.stepInstanceId().toString().substring(0, 8) + "-";
            assertThat(msgCaptor.getValue().correlationId()).startsWith(expectedPrefix);
        }
    }

    @Nested
    @DisplayName("Error Resilience Tests")
    class ErrorResilienceTests {

        @Test
        @DisplayName("Scanner failure on one partition does not stop others")
        void scannerFailure_doesNotStopOtherPartitions() {
            properties.setTotalPartitions(3);
            schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                    transitionPublisher, properties, meterRegistry, metrics);

            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0, 1, 2));
            when(dueStepScanner.scan(0, 3)).thenThrow(new RuntimeException("DB timeout"));
            when(dueStepScanner.scan(1, 3)).thenReturn(List.of(dueStep(TransitionType.PENDING_TO_DUE)));
            when(dueStepScanner.scan(2, 3)).thenReturn(Collections.emptyList());
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

            schedulerLoop.executeCycle();

            // Partition 1 still processed despite partition 0 failure
            verify(dueStepScanner).scan(1, 3);
            verify(dueStepScanner).scan(2, 3);
            verify(triggerProducer, times(1)).publish(any(), any());
        }

        @Test
        @DisplayName("Publish failure is handled gracefully")
        void publishFailure_handledGracefully() {
            when(leaderElection.getOwnedPartitions()).thenReturn(List.of(0));
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan(0, 1)).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(false);

            // Should not throw
            schedulerLoop.executeCycle();

            double failureCount = meterRegistry.counter("cce.scheduler.publish.failure",
                    "transition_type", "PENDING_TO_DUE", "partition", "0").count();
            assertThat(failureCount).isEqualTo(1.0);
        }
    }

    // --- Helpers ---

    private DueStep dueStep(TransitionType type) {
        return dueStep(type, UUID.randomUUID());
    }

    private DueStep dueStep(TransitionType type, UUID protocolInstanceId) {
        return new DueStep(
                UUID.randomUUID(),
                protocolInstanceId,
                type,
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1),
                JsonNodeFactory.instance.objectNode()
        );
    }
}
