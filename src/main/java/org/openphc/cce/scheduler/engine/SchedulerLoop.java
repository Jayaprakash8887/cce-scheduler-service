package org.openphc.cce.scheduler.engine;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.ObservabilityConfig;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.leader.LeaderElection;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class SchedulerLoop {

    private final LeaderElection leaderElection;
    private final DueStepScanner dueStepScanner;
    private final TransitionPublisher transitionPublisher;
    private final SchedulerProperties properties;
    private final MeterRegistry meterRegistry;
    private final ObservabilityConfig metrics;
    private final PartitionCursorService partitionCursor;

    public SchedulerLoop(LeaderElection leaderElection,
                         DueStepScanner dueStepScanner,
                         TransitionPublisher transitionPublisher,
                         SchedulerProperties properties,
                         MeterRegistry meterRegistry,
                         ObservabilityConfig metrics,
                         PartitionCursorService partitionCursor) {
        this.leaderElection = leaderElection;
        this.dueStepScanner = dueStepScanner;
        this.transitionPublisher = transitionPublisher;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.metrics = metrics;
        this.partitionCursor = partitionCursor;
    }

    @Scheduled(fixedDelayString = "${cce.scheduler.scan-interval}")
    public void executeCycle() {
        List<Integer> ownedPartitions = leaderElection.getOwnedPartitions();

        MDC.put("leaderStatus", ownedPartitions.isEmpty() ? "standby" : "leader");
        MDC.put("ownedPartitions", ownedPartitions.toString());
        MDC.put("totalPartitions", String.valueOf(properties.getTotalPartitions()));

        try {
            if (ownedPartitions.isEmpty()) {
                log.debug("Standby — no partitions owned, skipping cycle");
                return;
            }

            for (int partitionIndex : ownedPartitions) {
                processPartition(partitionIndex);
            }
        } finally {
            MDC.clear();
        }
    }

    private void processPartition(int partitionIndex) {
        String partition = String.valueOf(partitionIndex);
        MDC.put("currentPartition", partition);
        try {
            Timer.Sample timerSample = Timer.start(meterRegistry);

            List<DueStep> dueSteps = dueStepScanner.scan(partitionIndex, properties.getTotalPartitions());

            // Record scan steps metric
            metrics.scanStepsCounter("all", partition)
                    .increment(dueSteps.size());

            // Publish transitions
            int published = transitionPublisher.publishAll(dueSteps, partitionIndex);

            // Commit scan progress: advance the watermark past the emitted crossings
            advanceWatermark(partitionIndex, dueSteps, published);

            // Record scan duration
            timerSample.stop(metrics.scanDurationTimer(partition));

            // Record cycle count
            metrics.cycleCounter(partition).increment();

            log.info("Cycle complete — partition={}, scanned={}, published={}",
                    partitionIndex, dueSteps.size(), published);

        } catch (Exception e) {
            log.warn("Error processing partition {} — cycle continues", partitionIndex, e);
            metrics.cycleCounter(partition).increment();
        } finally {
            MDC.remove("currentPartition");
        }
    }

    /**
     * Advances the partition cursor to the last emitted {@code (threshold, id)} this
     * cycle, but only when the whole batch published successfully. A partial failure
     * leaves the cursor untouched so the failed (and re-published) crossings are
     * retried next cycle — transient Kafka failures never strand a batch. Empty cycles
     * do not advance.
     *
     * <p>The last element is taken by the scan's {@code ORDER BY (threshold, id)} order
     * (preserved through the repository/list), NOT by re-sorting in Java — Postgres and
     * {@code UUID.compareTo} order UUIDs differently, so only the DB order is authoritative.
     */
    private void advanceWatermark(int partitionIndex, List<DueStep> dueSteps, int published) {
        if (!properties.isWatermarkEnabled() || dueSteps.isEmpty() || published != dueSteps.size()) {
            return;
        }
        DueStep last = dueSteps.get(dueSteps.size() - 1);
        partitionCursor.advanceWatermark(partitionIndex, last.thresholdDate(), last.stepInstanceId());
        log.debug("Advanced cursor for partition {} to ({},{})",
                partitionIndex, last.thresholdDate(), last.stepInstanceId());
    }
}
