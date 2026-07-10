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

    public Timer scanDurationTimer() {
        return timerCache.computeIfAbsent("scan.duration", k ->
                Timer.builder("cce.scheduler.scan.duration")
                        .description("Time spent per scan cycle")
                        .register(registry));
    }

    public Counter scanStepsCounter(String transitionType) {
        return counterCache.computeIfAbsent("scan.steps-" + transitionType, k ->
                Counter.builder("cce.scheduler.scan.steps")
                        .tag("transition_type", transitionType)
                        .description("Number of steps found per transition type")
                        .register(registry));
    }

    public Counter publishSuccessCounter(String transitionType) {
        return counterCache.computeIfAbsent("publish.success-" + transitionType, k ->
                Counter.builder("cce.scheduler.publish.success")
                        .tag("transition_type", transitionType)
                        .description("Successful Kafka publishes")
                        .register(registry));
    }

    public Counter publishFailureCounter(String transitionType) {
        return counterCache.computeIfAbsent("publish.failure-" + transitionType, k ->
                Counter.builder("cce.scheduler.publish.failure")
                        .tag("transition_type", transitionType)
                        .description("Failed Kafka publishes")
                        .register(registry));
    }

    public Counter cycleCounter() {
        return counterCache.computeIfAbsent("cycle.count", k ->
                Counter.builder("cce.scheduler.cycle.count")
                        .description("Total scan cycles executed")
                        .register(registry));
    }
}
