package org.openphc.cce.scheduler.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchedulerTriggerProducer#publish} with a mocked {@link KafkaTemplate}.
 *
 * <p>Note: the pre-existing {@code SchedulerTriggerProducerTest} actually exercises
 * {@code TransitionPublisher} (it mocks this producer); the producer's own send/ack/failure logic
 * was untested. This class covers it directly.
 */
@ExtendWith(MockitoExtension.class)
class SchedulerTriggerProducerPublishTest {

    private static final String TOPIC = "cce.scheduler.triggers";

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private SchedulerTriggerProducer producer;

    @BeforeEach
    void setUp() {
        producer = new SchedulerTriggerProducer(kafkaTemplate, TOPIC);
    }

    private SchedulerTriggerMessage message(UUID stepId) {
        return new SchedulerTriggerMessage(
                stepId,
                TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC),
                "sched-PENDING_TO_DUE-corr");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void publish_whenSendSucceeds_returnsTrueAndUsesProtocolInstanceIdAsKey() {
        UUID protocolInstanceId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();

        SendResult<String, Object> sendResult = mock(SendResult.class);
        RecordMetadata metadata =
                new RecordMetadata(new TopicPartition(TOPIC, 3), 0L, 0, 0L, 0, 0);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        boolean ok = producer.publish(protocolInstanceId, message(stepId));

        assertThat(ok).isTrue();

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TOPIC);
        assertThat(captor.getValue().key()).isEqualTo(protocolInstanceId.toString());
    }

    @Test
    void publish_whenSendFails_returnsFalse() {
        UUID protocolInstanceId = UUID.randomUUID();
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        boolean ok = producer.publish(protocolInstanceId, message(UUID.randomUUID()));

        assertThat(ok).isFalse();
    }

    @Test
    void publish_whenTemplateThrowsSynchronously_returnsFalse() {
        UUID protocolInstanceId = UUID.randomUUID();
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new RuntimeException("serialization error"));

        boolean ok = producer.publish(protocolInstanceId, message(UUID.randomUUID()));

        assertThat(ok).isFalse();
    }
}
