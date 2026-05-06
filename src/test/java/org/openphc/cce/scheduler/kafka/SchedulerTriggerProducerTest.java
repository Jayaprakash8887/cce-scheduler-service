package org.openphc.cce.scheduler.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.engine.DueStep;
import org.openphc.cce.scheduler.engine.TransitionPublisher;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchedulerTriggerProducerTest {

    @Mock
    private SchedulerTriggerProducer producer;

    private TransitionPublisher transitionPublisher;
    private SimpleMeterRegistry meterRegistry;
    private ObservabilityConfig metrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metrics = new ObservabilityConfig(meterRegistry);
        transitionPublisher = new TransitionPublisher(producer, metrics);
    }

    @Test
    void publishAll_singleStep_publishesWithCorrectKey() {
        UUID stepId = UUID.randomUUID();
        UUID protocolId = UUID.randomUUID();
        DueStep dueStep = new DueStep(stepId, protocolId, TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

        transitionPublisher.publishAll(List.of(dueStep), 0);

        ArgumentCaptor<UUID> keyCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
        verify(producer).publish(keyCaptor.capture(), msgCaptor.capture());

        assertThat(keyCaptor.getValue()).isEqualTo(protocolId);
        assertThat(msgCaptor.getValue().stepInstanceId()).isEqualTo(stepId);
        assertThat(msgCaptor.getValue().transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
        assertThat(msgCaptor.getValue().triggeredAt()).isNotNull();
    }

    @Test
    void publishAll_correlationId_followsFormat() {
        UUID stepId = UUID.randomUUID();
        DueStep dueStep = new DueStep(stepId, UUID.randomUUID(), TransitionType.DUE_TO_OVERDUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

        transitionPublisher.publishAll(List.of(dueStep), 0);

        ArgumentCaptor<SchedulerTriggerMessage> msgCaptor = ArgumentCaptor.forClass(SchedulerTriggerMessage.class);
        verify(producer).publish(any(), msgCaptor.capture());

        String expectedPrefix = "sched-DUE_TO_OVERDUE-" + stepId.toString().substring(0, 8) + "-";
        assertThat(msgCaptor.getValue().correlationId()).startsWith(expectedPrefix);
    }

    @Test
    void publishAll_multipleSteps_publishesEach() {
        DueStep step1 = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());
        DueStep step2 = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.DUE_TO_OVERDUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());
        DueStep step3 = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.OVERDUE_TO_MISSED,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

        int result = transitionPublisher.publishAll(List.of(step1, step2, step3), 0);

        assertThat(result).isEqualTo(3);
        verify(producer, times(3)).publish(any(), any());
    }

    @Test
    void publishAll_emptyList_publishesNothing() {
        int result = transitionPublisher.publishAll(List.of(), 0);

        assertThat(result).isEqualTo(0);
        verify(producer, never()).publish(any(), any());
    }

    @Test
    void publishAll_onFailure_incrementsFailureMetric() {
        DueStep dueStep = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(false);

        int result = transitionPublisher.publishAll(List.of(dueStep), 0);

        assertThat(result).isEqualTo(0);
        double failureCount = meterRegistry.counter("cce.scheduler.publish.failure",
                "transition_type", "PENDING_TO_DUE", "partition", "0").count();
        assertThat(failureCount).isEqualTo(1.0);
    }

    @Test
    void publishAll_onSuccess_incrementsSuccessMetric() {
        DueStep dueStep = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.OVERDUE_TO_MISSED,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class))).thenReturn(true);

        transitionPublisher.publishAll(List.of(dueStep), 2);

        double successCount = meterRegistry.counter("cce.scheduler.publish.success",
                "transition_type", "OVERDUE_TO_MISSED", "partition", "2").count();
        assertThat(successCount).isEqualTo(1.0);
    }

    @Test
    void publishAll_mixedResults_tracksCorrectly() {
        DueStep success = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());
        DueStep failure = new DueStep(UUID.randomUUID(), UUID.randomUUID(), TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        when(producer.publish(any(UUID.class), any(SchedulerTriggerMessage.class)))
                .thenReturn(true)
                .thenReturn(false);

        int result = transitionPublisher.publishAll(List.of(success, failure), 0);

        assertThat(result).isEqualTo(1);
    }

    @Test
    void buildCorrelationId_format() {
        UUID stepId = UUID.fromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        DueStep step = new DueStep(stepId, UUID.randomUUID(), TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC), JsonNodeFactory.instance.objectNode());

        String correlationId = TransitionPublisher.buildCorrelationId(step);

        assertThat(correlationId).startsWith("sched-PENDING_TO_DUE-a1b2c3d4-");
    }
}
