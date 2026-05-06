package org.openphc.cce.scheduler.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories(basePackages = "org.openphc.cce.scheduler.domain.repository")
public class JpaConfig {
}
