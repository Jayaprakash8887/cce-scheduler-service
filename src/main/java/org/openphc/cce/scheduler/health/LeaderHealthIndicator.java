package org.openphc.cce.scheduler.health;

import org.openphc.cce.scheduler.leader.LeaderElection;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class LeaderHealthIndicator implements HealthIndicator {

    private final LeaderElection leaderElection;

    public LeaderHealthIndicator(LeaderElection leaderElection) {
        this.leaderElection = leaderElection;
    }

    @Override
    public Health health() {
        // Always UP: a standby is healthy and ready to take over — only not currently leading.
        // The `isLeader` detail distinguishes leader from standby without failing readiness,
        // so every replica stays in rotation for fast failover.
        Health.Builder builder = Health.up();

        builder.withDetail("leaderId", leaderElection.getLeaderId())
                .withDetail("isLeader", leaderElection.isLeader());

        if (leaderElection.getLastHeartbeat() != null) {
            builder.withDetail("lastHeartbeat", leaderElection.getLastHeartbeat().toString());
        }
        if (leaderElection.getLeaseExpiresAt() != null) {
            builder.withDetail("leaseExpiresAt", leaderElection.getLeaseExpiresAt().toString());
        }

        return builder.build();
    }
}
