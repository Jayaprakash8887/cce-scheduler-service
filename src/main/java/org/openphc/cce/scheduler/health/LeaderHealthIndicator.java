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
        Health.Builder builder = leaderElection.isLeader() ? Health.up() : Health.outOfService();

        builder.withDetail("leaderId", leaderElection.getLeaderId())
                .withDetail("isLeader", leaderElection.isLeader())
                .withDetail("ownedPartitions", leaderElection.getOwnedPartitions())
                .withDetail("totalPartitions", leaderElection.getTotalPartitions());

        if (leaderElection.getLastHeartbeat() != null) {
            builder.withDetail("lastHeartbeat", leaderElection.getLastHeartbeat().toString());
        }
        if (leaderElection.getLeaseExpiresAt() != null) {
            builder.withDetail("leaseExpiresAt", leaderElection.getLeaseExpiresAt().toString());
        }

        return builder.build();
    }
}
