package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;
import org.openphc.cce.scheduler.domain.repository.SchedulerNodeRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderElectionTest {

    @Mock
    private SchedulerLeaseRepository leaseRepository;

    @Mock
    private SchedulerNodeRepository nodeRepository;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    private SchedulerProperties properties;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setTotalPartitions(1);
        properties.setLockAcquireDelayMs(0);
        properties.setLeaseDurationSeconds(30);
        properties.setAdvisoryLockKey(100001);
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    void initialState_noPartitionsOwned() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        assertThat(election.isLeader()).isFalse();
        assertThat(election.getOwnedPartitions()).isEmpty();
        assertThat(election.getLeaderId()).isNotBlank();
    }

    @Test
    void totalPartitions_reflectsConfig() {
        properties.setTotalPartitions(4);
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        assertThat(election.getTotalPartitions()).isEqualTo(4);
    }

    @Test
    void leaderId_isUniquePerInstance() {
        LeaderElection election1 = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);
        LeaderElection election2 = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        assertThat(election1.getLeaderId()).isNotEqualTo(election2.getLeaderId());
    }

    @Test
    void shutdown_clearsOwnedPartitions() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        election.shutdown();

        assertThat(election.getOwnedPartitions()).isEmpty();
        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void tryAcquirePartitions_withFailedConnection_setsEmptyPartitions() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection refused"));

        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        election.tryAcquirePartitions();

        assertThat(election.isLeader()).isFalse();
        assertThat(election.getOwnedPartitions()).isEmpty();
    }

    @Test
    void sleepJitter_withZeroDelay_doesNotBlock() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        long start = System.currentTimeMillis();
        election.sleepJitter(0);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed).isLessThan(50);
    }

    @Test
    void sleepJitter_withPositiveDelay_sleepsWithinBound() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        long start = System.currentTimeMillis();
        election.sleepJitter(100);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed).isLessThan(150);
    }

    @Test
    void leaderStatusGauge_registeredPerPartition() {
        properties.setTotalPartitions(3);
        new LeaderElection(properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "0").gauge()).isNotNull();
        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "1").gauge()).isNotNull();
        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "2").gauge()).isNotNull();
    }

    @Test
    void fairShare_singleNode_ownsAllPartitions() {
        assertThat(LeaderElection.fairShare(3, 1)).isEqualTo(3);
    }

    @Test
    void fairShare_evenSplit_isOnePerNode() {
        // 3 partitions across 3 pods -> 1 each (the distribution this fixes).
        assertThat(LeaderElection.fairShare(3, 3)).isEqualTo(1);
    }

    @Test
    void fairShare_unevenSplit_roundsUpForFullCoverage() {
        // 4 partitions across 3 pods -> ceil(1.33)=2, guaranteeing no orphaned partition.
        assertThat(LeaderElection.fairShare(4, 3)).isEqualTo(2);
    }

    @Test
    void fairShare_moreNodesThanPartitions_capsAtOne() {
        assertThat(LeaderElection.fairShare(3, 5)).isEqualTo(1);
    }

    @Test
    void fairShare_treatsZeroNodesAsSolo() {
        // Defensive: a count below 1 cannot starve the only live pod.
        assertThat(LeaderElection.fairShare(3, 0)).isEqualTo(3);
    }

    @Test
    void tryAcquirePartitions_rebalancesToFairShareAsClusterGrows() throws SQLException {
        properties.setTotalPartitions(3);
        properties.setLockAcquireDelayMs(0);

        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isClosed()).thenReturn(false);
        when(connection.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true); // every advisory lock is grabbable
        when(leaseRepository.findByPartitionIndex(anyInt())).thenReturn(Optional.empty());
        when(leaseRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(nodeRepository.findById(anyString())).thenReturn(Optional.empty());
        when(nodeRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        // Cluster looks like 1 node on the first cycle, then 3 once peers have joined.
        when(nodeRepository.countByLastHeartbeatAfter(any())).thenReturn(1L, 3L);

        LeaderElection election = new LeaderElection(
                properties, leaseRepository, nodeRepository, dataSource, meterRegistry);

        // Round 1: booted alone -> fair share is all 3 partitions.
        election.tryAcquirePartitions();
        assertThat(election.getOwnedPartitions()).containsExactly(0, 1, 2);

        // Round 2: two peers joined -> fair share drops to 1, surplus released.
        election.tryAcquirePartitions();
        assertThat(election.getOwnedPartitions()).hasSize(1);
    }
}
