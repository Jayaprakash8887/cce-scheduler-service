package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderElectionTest {

    @Mock
    private SchedulerLeaseRepository leaseRepository;

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
                properties, leaseRepository, dataSource, meterRegistry);

        assertThat(election.isLeader()).isFalse();
        assertThat(election.getOwnedPartitions()).isEmpty();
        assertThat(election.getLeaderId()).isNotBlank();
    }

    @Test
    void totalPartitions_reflectsConfig() {
        properties.setTotalPartitions(4);
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        assertThat(election.getTotalPartitions()).isEqualTo(4);
    }

    @Test
    void leaderId_isUniquePerInstance() {
        LeaderElection election1 = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);
        LeaderElection election2 = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        assertThat(election1.getLeaderId()).isNotEqualTo(election2.getLeaderId());
    }

    @Test
    void shutdown_clearsOwnedPartitions() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        election.shutdown();

        assertThat(election.getOwnedPartitions()).isEmpty();
        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void tryAcquirePartitions_withFailedConnection_setsEmptyPartitions() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection refused"));

        LeaderElection election = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        election.tryAcquirePartitions();

        assertThat(election.isLeader()).isFalse();
        assertThat(election.getOwnedPartitions()).isEmpty();
    }

    @Test
    void sleepJitter_withZeroDelay_doesNotBlock() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        long start = System.currentTimeMillis();
        election.sleepJitter(0);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed).isLessThan(50);
    }

    @Test
    void sleepJitter_withPositiveDelay_sleepsWithinBound() {
        LeaderElection election = new LeaderElection(
                properties, leaseRepository, dataSource, meterRegistry);

        long start = System.currentTimeMillis();
        election.sleepJitter(100);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed).isLessThan(150);
    }

    @Test
    void leaderStatusGauge_registeredPerPartition() {
        properties.setTotalPartitions(3);
        new LeaderElection(properties, leaseRepository, dataSource, meterRegistry);

        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "0").gauge()).isNotNull();
        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "1").gauge()).isNotNull();
        assertThat(meterRegistry.find("cce.scheduler.leader.status")
                .tag("partition", "2").gauge()).isNotNull();
    }
}
