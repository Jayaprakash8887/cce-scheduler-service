package org.openphc.cce.scheduler.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class ObservabilityConfig {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> counterCache = new ConcurrentHashMap<>();

    public ObservabilityConfig(MeterRegistry registry) {
        this.registry = registry;
    }

    public Timer scanDurationTimer(String partition) {
        return timerCache.computeIfAbsent("scan.duration-" + partition, k ->
                Timer.builder("cce.scheduler.scan.duration")
                        .tag("partition", partition)
                        .description("Time spent per scan cycle")
                        .register(registry));
    }

    public Counter scanStepsCounter(String transitionType, String partition) {
        return counterCache.computeIfAbsent("scan.steps-" + transitionType + "-" + partition, k ->
                Counter.builder("cce.scheduler.scan.steps")
                        .tag("transition_type", transitionType)
                        .tag("partition", partition)
                        .description("Number of steps found per transition type")
                        .register(registry));
    }

    public Counter publishSuccessCounter(String transitionType, String partition) {
        return counterCache.computeIfAbsent("publish.success-" + transitionType + "-" + partition, k ->
                Counter.builder("cce.scheduler.publish.success")
                        .tag("transition_type", transitionType)
                        .tag("partition", partition)
                        .description("Successful Kafka publishes")
                        .register(registry));
    }

    public Counter publishFailureCounter(String transitionType, String partition) {
        return counterCache.computeIfAbsent("publish.failure-" + transitionType + "-" + partition, k ->
                Counter.builder("cce.scheduler.publish.failure")
                        .tag("transition_type", transitionType)
                        .tag("partition", partition)
                        .description("Failed Kafka publishes")
                        .register(registry));
    }

    public Counter cycleCounter(String partition) {
        return counterCache.computeIfAbsent("cycle.count-" + partition, k ->
                Counter.builder("cce.scheduler.cycle.count")
                        .tag("partition", partition)
                        .description("Total scan cycles executed")
                        .register(registry));
    }
}
