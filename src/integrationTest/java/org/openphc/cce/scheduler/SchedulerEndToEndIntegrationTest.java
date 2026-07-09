package org.openphc.cce.scheduler;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.openphc.cce.scheduler.engine.PartitionCursorService;
import org.openphc.cce.scheduler.engine.SchedulerLoop;
import org.openphc.cce.scheduler.engine.TransitionPublisher;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerMessage;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerProducer;
import org.openphc.cce.scheduler.leader.LeaderElection;
import org.springframework.kafka.support.SendResult;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end tests for the single-leader, single-partition pipeline using mocks:
 * leader guard → scan → async publish → metrics.
 */
@ExtendWith(MockitoExtension.class)
class SchedulerEndToEndIntegrationTest {

    @Mock
    private LeaderElection leaderElection;

    @Mock
    private DueStepScanner dueStepScanner;

    @Mock
    private SchedulerTriggerProducer triggerProducer;

    @Mock
    private PartitionCursorService partitionCursor;

    private SchedulerProperties properties;
    private SimpleMeterRegistry meterRegistry;
    private ObservabilityConfig metrics;
    private TransitionPublisher transitionPublisher;
    private SchedulerLoop schedulerLoop;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setBatchSize(100);
        meterRegistry = new SimpleMeterRegistry();
        metrics = new ObservabilityConfig(meterRegistry);
        transitionPublisher = new TransitionPublisher(triggerProducer, metrics);
        schedulerLoop = new SchedulerLoop(leaderElection, dueStepScanner,
                transitionPublisher, properties, meterRegistry, metrics, partitionCursor);
    }

    private CompletableFuture<SendResult<String, Object>> ok() {
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<SendResult<String, Object>> failed() {
        CompletableFuture<SendResult<String, Object>> f = new CompletableFuture<>();
        f.completeExceptionally(new RuntimeException("broker down"));
        return f;
    }

    @Nested
    @DisplayName("State Transition Tests")
    class StateTransitionTests {

        @Test
        @DisplayName("PENDING step → PENDING_TO_DUE published")
        void pendingStep_publishesPendingToDue() {
            when(leaderElection.isLeader()).thenReturn(true);
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
            assertThat(msgCaptor.getValue().stepInstanceId()).isEqualTo(step.stepInstanceId());
        }

        @Test
        @DisplayName("DUE step → DUE_TO_OVERDUE published")
        void dueStep_publishesDueToOverdue() {
            when(leaderElection.isLeader()).thenReturn(true);
            DueStep step = dueStep(TransitionType.DUE_TO_OVERDUE);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.DUE_TO_OVERDUE);
        }

        @Test
        @DisplayName("OVERDUE step → OVERDUE_TO_MISSED published")
        void overdueStep_publishesOverdueToMissed() {
            when(leaderElection.isLeader()).thenReturn(true);
            DueStep step = dueStep(TransitionType.OVERDUE_TO_MISSED);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

            schedulerLoop.executeCycle();

            ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
            verify(triggerProducer).publish(eq(step.protocolInstanceId()), msgCaptor.capture());
            assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
        }

        @Test
        @DisplayName("Terminal (COMPLETED) step is never picked up — scanner returns empty")
        void completedStep_neverPublished() {
            when(leaderElection.isLeader()).thenReturn(true);
            when(dueStepScanner.scan()).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(triggerProducer, never()).publish(any(UUID.class), any(SchedulerTriggerMessage.class));
        }
    }

    @Nested
    @DisplayName("Leadership Tests")
    class LeadershipTests {

        @Test
        @DisplayName("Leader processes the cycle")
        void leader_processes() {
            when(leaderElection.isLeader()).thenReturn(true);
            when(dueStepScanner.scan()).thenReturn(List.of(dueStep(TransitionType.PENDING_TO_DUE)));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

            schedulerLoop.executeCycle();

            verify(dueStepScanner, times(1)).scan();
            verify(triggerProducer, times(1)).publish(any(), any());
        }

        @Test
        @DisplayName("Standby (not leader) does nothing")
        void standby_doesNothing() {
            when(leaderElection.isLeader()).thenReturn(false);

            schedulerLoop.executeCycle();

            verify(dueStepScanner, never()).scan();
            verify(triggerProducer, never()).publish(any(), any());
        }
    }

    @Nested
    @DisplayName("Empty & Metrics Tests")
    class EmptyAndMetricsTests {

        @Test
        @DisplayName("Empty scan cycle → no messages, metrics still recorded")
        void emptyScanCycle_noMessages_metricsRecorded() {
            when(leaderElection.isLeader()).thenReturn(true);
            when(dueStepScanner.scan()).thenReturn(Collections.emptyList());

            schedulerLoop.executeCycle();

            verify(triggerProducer, never()).publish(any(UUID.class), any(SchedulerTriggerMessage.class));
            assertThat(meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count())
                    .isEqualTo(1.0);
            assertThat(meterRegistry.timer("cce.scheduler.scan.duration", "partition", "0").count())
                    .isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Kafka Message Key Tests")
    class KafkaKeyTests {

        @Test
        @DisplayName("Kafka key is protocolInstanceId")
        void kafkaKey_isProtocolInstanceId() {
            when(leaderElection.isLeader()).thenReturn(true);
            UUID protocolId = UUID.randomUUID();
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

            schedulerLoop.executeCycle();

            verify(triggerProducer).publish(eq(protocolId), any(SchedulerTriggerMessage.class));
        }

        @Test
        @DisplayName("Multiple steps from same protocol use same key")
        void multipleSteps_sameProtocol_sameKey() {
            when(leaderElection.isLeader()).thenReturn(true);
            UUID protocolId = UUID.randomUUID();
            DueStep step1 = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            DueStep step2 = dueStep(TransitionType.PENDING_TO_DUE, protocolId);
            when(dueStepScanner.scan()).thenReturn(List.of(step1, step2));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

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
            when(leaderElection.isLeader()).thenReturn(true);
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(ok());

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
        @DisplayName("Scanner failure does not terminate the scheduled task")
        void scannerFailure_doesNotTerminate() {
            when(leaderElection.isLeader()).thenReturn(true);
            when(dueStepScanner.scan()).thenThrow(new RuntimeException("DB timeout"));

            schedulerLoop.executeCycle(); // should not throw

            assertThat(meterRegistry.counter("cce.scheduler.cycle.count", "partition", "0").count())
                    .isEqualTo(1.0);
        }

        @Test
        @DisplayName("Publish failure is handled gracefully and counted")
        void publishFailure_handledGracefully() {
            when(leaderElection.isLeader()).thenReturn(true);
            DueStep step = dueStep(TransitionType.PENDING_TO_DUE);
            when(dueStepScanner.scan()).thenReturn(List.of(step));
            when(triggerProducer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(failed());

            schedulerLoop.executeCycle(); // should not throw

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
