package org.openphc.cce.scheduler.health;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.leader.LeaderElection;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LeaderHealthIndicator}, which maps {@link LeaderElection} state onto an
 * actuator {@link Health} report — UP when this node is leader, OUT_OF_SERVICE otherwise — with the
 * optional heartbeat/lease timestamps included only when present.
 */
@ExtendWith(MockitoExtension.class)
class LeaderHealthIndicatorTest {

    @Mock
    private LeaderElection leaderElection;

    private void stubCommon() {
        lenient().when(leaderElection.getLeaderId()).thenReturn("node-1");
    }

    @Test
    void health_whenLeader_returnsUpWithDetails() {
        stubCommon();
        when(leaderElection.isLeader()).thenReturn(true);

        Health health = new LeaderHealthIndicator(leaderElection).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("leaderId", "node-1")
                .containsEntry("isLeader", true);
    }

    @Test
    void health_whenStandby_returnsUpButNotLeader() {
        stubCommon();
        when(leaderElection.isLeader()).thenReturn(false);

        Health health = new LeaderHealthIndicator(leaderElection).health();

        // Standby stays UP (ready for failover); isLeader marks it as not currently leading.
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("isLeader", false);
    }

    @Test
    void health_whenHeartbeatAndLeasePresent_includesTimestamps() {
        stubCommon();
        when(leaderElection.isLeader()).thenReturn(true);
        OffsetDateTime hb = OffsetDateTime.of(2026, 6, 30, 10, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime lease = hb.plusSeconds(30);
        when(leaderElection.getLastHeartbeat()).thenReturn(hb);
        when(leaderElection.getLeaseExpiresAt()).thenReturn(lease);

        Health health = new LeaderHealthIndicator(leaderElection).health();

        assertThat(health.getDetails())
                .containsEntry("lastHeartbeat", hb.toString())
                .containsEntry("leaseExpiresAt", lease.toString());
    }

    @Test
    void health_whenHeartbeatAndLeaseNull_omitsTimestamps() {
        stubCommon();
        when(leaderElection.isLeader()).thenReturn(true);
        when(leaderElection.getLastHeartbeat()).thenReturn(null);
        when(leaderElection.getLeaseExpiresAt()).thenReturn(null);

        Health health = new LeaderHealthIndicator(leaderElection).health();

        assertThat(health.getDetails())
                .doesNotContainKey("lastHeartbeat")
                .doesNotContainKey("leaseExpiresAt");
    }
}
