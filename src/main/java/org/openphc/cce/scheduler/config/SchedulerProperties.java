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
    private long scanInterval = 10000;

    @Min(1)
    @Max(1000)
    private int batchSize = 1000;

    @Min(10)
    @Max(300)
    private int leaseDurationSeconds = 30;

    @Min(1000)
    @Max(60000)
    private long leaderRetryInterval = 5000;

    private long advisoryLockKey = 100001;
}
