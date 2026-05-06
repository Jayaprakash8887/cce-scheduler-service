package org.openphc.cce.scheduler.engine;

import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerMessage;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerProducer;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Component
@Slf4j
public class TransitionPublisher {

    private final SchedulerTriggerProducer producer;
    private final ObservabilityConfig metrics;

    public TransitionPublisher(SchedulerTriggerProducer producer, ObservabilityConfig metrics) {
        this.producer = producer;
        this.metrics = metrics;
    }

    /**
     * Publishes transition messages for all due steps and tracks success/failure metrics.
     * Returns the count of successfully published messages.
     */
    public int publishAll(List<DueStep> dueSteps, int partitionIndex) {
        String partition = String.valueOf(partitionIndex);
        int successCount = 0;

        for (DueStep step : dueSteps) {
            String correlationId = buildCorrelationId(step);

            MDC.put("correlationId", correlationId);
            MDC.put("stepInstanceId", step.stepInstanceId().toString());
            MDC.put("transitionType", step.transitionType().name());

            try {
                SchedulerTriggerMessage message = new SchedulerTriggerMessage(
                        step.stepInstanceId(),
                        step.transitionType(),
                        OffsetDateTime.now(ZoneOffset.UTC),
                        correlationId
                );

                log.debug("Publishing transition: {} for step {}",
                        step.transitionType(), step.stepInstanceId());

                boolean success = producer.publish(step.protocolInstanceId(), message);

                if (success) {
                    successCount++;
                    metrics.publishSuccessCounter(step.transitionType().name(), partition).increment();
                } else {
                    log.warn("Failed to publish transition {} for step {}",
                            step.transitionType(), step.stepInstanceId());
                    metrics.publishFailureCounter(step.transitionType().name(), partition).increment();
                }
            } finally {
                MDC.remove("correlationId");
                MDC.remove("stepInstanceId");
                MDC.remove("transitionType");
            }
        }

        log.info("Published {}/{} transitions for partition {}",
                successCount, dueSteps.size(), partitionIndex);
        return successCount;
    }

    public static String buildCorrelationId(DueStep step) {
        String stepIdPrefix = step.stepInstanceId().toString().substring(0, 8);
        long ts = System.currentTimeMillis();
        return "sched-" + step.transitionType().name() + "-" + stepIdPrefix + "-" + ts;
    }
}
