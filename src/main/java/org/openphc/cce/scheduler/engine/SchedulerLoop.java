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
    private final ScanCursorService scanCursor;

    public SchedulerLoop(LeaderElection leaderElection,
                         DueStepScanner dueStepScanner,
                         TransitionPublisher transitionPublisher,
                         SchedulerProperties properties,
                         MeterRegistry meterRegistry,
                         ObservabilityConfig metrics,
                         ScanCursorService scanCursor) {
        this.leaderElection = leaderElection;
        this.dueStepScanner = dueStepScanner;
        this.transitionPublisher = transitionPublisher;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.metrics = metrics;
        this.scanCursor = scanCursor;
    }

    @Scheduled(fixedDelayString = "${cce.scheduler.scan-interval}")
    public void executeCycle() {
        boolean leader = leaderElection.isLeader();
        MDC.put("leaderStatus", leader ? "leader" : "standby");
        try {
            if (!leader) {
                log.debug("Standby — not leader, skipping cycle");
                return;
            }

            Timer.Sample timerSample = Timer.start(meterRegistry);

            List<DueStep> dueSteps = dueStepScanner.scan();
            metrics.scanStepsCounter("all").increment(dueSteps.size());

            int published = transitionPublisher.publishAll(dueSteps);

            // Commit scan progress: advance the watermark past the emitted crossings.
            advanceWatermark(dueSteps, published);

            timerSample.stop(metrics.scanDurationTimer());
            metrics.cycleCounter().increment();

            log.info("Cycle complete — scanned={}, published={}", dueSteps.size(), published);
        } catch (Exception e) {
            log.warn("Error in scan cycle — cycle continues", e);
            metrics.cycleCounter().increment();
        } finally {
            MDC.clear();
        }
    }

    /**
     * Advances the watermark to the last emitted {@code (threshold, id)} (the scan's
     * {@code ORDER BY (threshold, id)} order), only when the whole batch published
     * successfully. A partial failure leaves the cursor untouched so failed crossings
     * retry next cycle; empty cycles do not advance.
     */
    private void advanceWatermark(List<DueStep> dueSteps, int published) {
        if (dueSteps.isEmpty() || published != dueSteps.size()) {
            return;
        }
        DueStep last = dueSteps.get(dueSteps.size() - 1);
        scanCursor.advanceWatermark(last.thresholdDate(), last.stepInstanceId());
        log.debug("Advanced cursor to ({},{})", last.thresholdDate(), last.stepInstanceId());
    }
}
