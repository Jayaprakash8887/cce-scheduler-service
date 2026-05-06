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

    public SchedulerLoop(LeaderElection leaderElection,
                         DueStepScanner dueStepScanner,
                         TransitionPublisher transitionPublisher,
                         SchedulerProperties properties,
                         MeterRegistry meterRegistry,
                         ObservabilityConfig metrics) {
        this.leaderElection = leaderElection;
        this.dueStepScanner = dueStepScanner;
        this.transitionPublisher = transitionPublisher;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.metrics = metrics;
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
}
