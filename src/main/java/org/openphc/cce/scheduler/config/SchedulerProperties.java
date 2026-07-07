package org.openphc.cce.scheduler.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "cce.scheduler")
@Validated
@Getter
@Setter
public class SchedulerProperties {

    @Min(1000)
    @Max(300000)
    private long scanInterval = 5000;

    @Min(1)
    @Max(1000)
    private int batchSize = 100;

    @Min(10)
    @Max(300)
    private int leaseDurationSeconds = 30;

    @Min(1000)
    @Max(60000)
    private long leaderRetryInterval = 5000;

    private long advisoryLockKey = 100001;

    @Min(1)
    @Max(64)
    private int totalPartitions = 1;

    @Min(0)
    @Max(500)
    private int lockAcquireDelayMs = 50;

    /**
     * When true, each partition scan is bounded below by a persisted per-partition
     * watermark so already-emitted threshold crossings are not re-scanned every cycle.
     * When false, every cycle scans all due rows up to now (legacy behavior).
     */
    private boolean watermarkEnabled = true;

    /**
     * Grace period after startup during which an instance registers its heartbeat but
     * DEFERS acquiring partition locks, so co-starting peers can register first and the
     * initial ownership is spread by fair share instead of the first booter greedily
     * grabbing everything (then shedding). Only applied when {@code totalPartitions > 1};
     * {@code 0} disables it (default). The self-rebalance still converges without it — this
     * only smooths the initial distribution at the cost of a startup delay before scanning.
     */
    @Min(0)
    @Max(300000)
    private long startupAcquireDelayMs = 0;
}
