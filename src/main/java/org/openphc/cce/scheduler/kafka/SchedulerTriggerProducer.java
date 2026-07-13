package org.openphc.cce.scheduler.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class SchedulerTriggerProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public SchedulerTriggerProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${cce.kafka.topics.scheduler-triggers}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Sends a trigger message <b>asynchronously</b> with protocolInstanceId as the message
     * key, returning the in-flight future. The caller fires many of these and then awaits
     * the batch, so the Kafka producer pipelines/batches the sends instead of paying one
     * broker round-trip per message (the previous synchronous model).
     */
    public CompletableFuture<SendResult<String, Object>> publish(UUID protocolInstanceId,
                                                                 SchedulerTriggerMessage message) {
        String key = protocolInstanceId.toString();
        try {
            return kafkaTemplate.send(new ProducerRecord<>(topic, key, message))
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Published {} to partition {} offset {} — correlationId={}",
                                    message.transitionType(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset(),
                                    message.correlationId());
                        } else {
                            log.warn("Failed to publish {} for step {} — correlationId={}",
                                    message.transitionType(),
                                    message.stepInstanceId(),
                                    message.correlationId(),
                                    ex);
                        }
                    });
        } catch (Exception e) {
            // A synchronous failure (serialization, buffer exhaustion) must not break the
            // caller's batch loop — surface it as a failed future like an async failure.
            log.warn("Failed to submit publish for step {} — correlationId={}",
                    message.stepInstanceId(), message.correlationId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
}
