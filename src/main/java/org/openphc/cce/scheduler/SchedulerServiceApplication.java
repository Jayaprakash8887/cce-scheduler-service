package org.openphc.cce.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.openphc.cce.scheduler.config.SchedulerProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerServiceApplication.class, args);
    }
}
