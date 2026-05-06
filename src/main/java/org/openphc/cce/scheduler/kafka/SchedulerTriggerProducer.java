package org.openphc.cce.scheduler.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
     * Publishes a trigger message synchronously with protocolInstanceId as the message key.
     * Returns true on success, false on failure.
     */
    public boolean publish(UUID protocolInstanceId, SchedulerTriggerMessage message) {
        String key = protocolInstanceId.toString();
        try {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(new ProducerRecord<>(topic, key, message));

            SendResult<String, Object> result = future.get(5, TimeUnit.SECONDS);
            log.debug("Published {} to partition {} offset {} — correlationId={}",
                    message.transitionType(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    message.correlationId());
            return true;
        } catch (Exception e) {
            log.warn("Failed to publish {} for step {} — correlationId={}",
                    message.transitionType(),
                    message.stepInstanceId(),
                    message.correlationId(),
                    e);
            return false;
        }
    }
}
