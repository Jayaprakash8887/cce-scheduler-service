package org.openphc.cce.scheduler.engine;

import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerMessage;
import org.openphc.cce.scheduler.kafka.SchedulerTriggerProducer;
import org.slf4j.MDC;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class TransitionPublisher {

    /** Upper bound for awaiting a fired batch; each send resolves within delivery.timeout.ms (15s). */
    private static final long BATCH_AWAIT_TIMEOUT_SECONDS = 30;

    private static final String PARTITION = String.valueOf(PartitionCursorService.SINGLE_PARTITION);

    private final SchedulerTriggerProducer producer;
    private final ObservabilityConfig metrics;

    public TransitionPublisher(SchedulerTriggerProducer producer, ObservabilityConfig metrics) {
        this.producer = producer;
        this.metrics = metrics;
    }

    /**
     * Publishes transition messages for all due steps and returns the count of successes.
     *
     * <p>All sends are fired first (async, non-blocking) so the Kafka producer pipelines
     * and batches them; the batch is then awaited and each outcome tallied. This replaces
     * the old send-then-block-per-message loop and lifts throughput for bursty scans.
     */
    public int publishAll(List<DueStep> dueSteps) {
        if (dueSteps.isEmpty()) {
            return 0;
        }

        // 1) Fire every send without blocking, so the producer batches them.
        List<PendingSend> pending = new ArrayList<>(dueSteps.size());
        for (DueStep step : dueSteps) {
            String correlationId = buildCorrelationId(step);
            SchedulerTriggerMessage message = new SchedulerTriggerMessage(
                    step.stepInstanceId(),
                    step.transitionType(),
                    OffsetDateTime.now(ZoneOffset.UTC),
                    correlationId);
            pending.add(new PendingSend(step, correlationId,
                    producer.publish(step.protocolInstanceId(), message)));
        }

        // 2) Await the batch and tally per-message outcomes.
        int successCount = 0;
        for (PendingSend p : pending) {
            MDC.put("correlationId", p.correlationId());
            MDC.put("stepInstanceId", p.step().stepInstanceId().toString());
            MDC.put("transitionType", p.step().transitionType().name());
            try {
                p.future().get(BATCH_AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                successCount++;
                metrics.publishSuccessCounter(p.step().transitionType().name(), PARTITION).increment();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                metrics.publishFailureCounter(p.step().transitionType().name(), PARTITION).increment();
                log.warn("Interrupted awaiting publish for step {}", p.step().stepInstanceId());
            } catch (Exception e) {
                metrics.publishFailureCounter(p.step().transitionType().name(), PARTITION).increment();
                log.warn("Failed to publish transition {} for step {}",
                        p.step().transitionType(), p.step().stepInstanceId());
            } finally {
                MDC.remove("correlationId");
                MDC.remove("stepInstanceId");
                MDC.remove("transitionType");
            }
        }

        log.info("Published {}/{} transitions", successCount, dueSteps.size());
        return successCount;
    }

    public static String buildCorrelationId(DueStep step) {
        String stepIdPrefix = step.stepInstanceId().toString().substring(0, 8);
        long ts = System.currentTimeMillis();
        return "sched-" + step.transitionType().name() + "-" + stepIdPrefix + "-" + ts;
    }

    private record PendingSend(DueStep step, String correlationId,
                               CompletableFuture<SendResult<String, Object>> future) {}
}
