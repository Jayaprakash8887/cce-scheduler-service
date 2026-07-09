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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        properties.setLeaseDurationSeconds(30);
        properties.setAdvisoryLockKey(100001);
        meterRegistry = new SimpleMeterRegistry();
    }

    private LeaderElection newElection() {
        return new LeaderElection(properties, leaseRepository, dataSource, meterRegistry);
    }

    @Test
    void initialState_notLeader() {
        LeaderElection election = newElection();

        assertThat(election.isLeader()).isFalse();
        assertThat(election.getLeaderId()).isNotBlank();
    }

    @Test
    void leaderId_isUniquePerInstance() {
        assertThat(newElection().getLeaderId()).isNotEqualTo(newElection().getLeaderId());
    }

    @Test
    void shutdown_clearsLeadership() {
        LeaderElection election = newElection();

        election.shutdown();

        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void tryAcquireLeadership_withFailedConnection_notLeader() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection refused"));

        LeaderElection election = newElection();
        election.tryAcquireLeadership();

        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void tryAcquireLeadership_lockAcquired_becomesLeader() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);
        when(leaseRepository.findByPartitionIndex(anyInt())).thenReturn(Optional.empty());
        when(leaseRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        LeaderElection election = newElection();
        election.tryAcquireLeadership();

        assertThat(election.isLeader()).isTrue();
        verify(leaseRepository).save(any());
    }

    @Test
    void tryAcquireLeadership_lockUnavailable_staysStandby() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(false); // lock held elsewhere

        LeaderElection election = newElection();
        election.tryAcquireLeadership();

        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void tryAcquireLeadership_alreadyLeader_doesNotReacquireLock() throws SQLException {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isClosed()).thenReturn(false);
        when(connection.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);
        when(leaseRepository.findByPartitionIndex(anyInt())).thenReturn(Optional.empty());
        when(leaseRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        LeaderElection election = newElection();
        election.tryAcquireLeadership();
        election.tryAcquireLeadership(); // second cycle: already holds the lock

        assertThat(election.isLeader()).isTrue();
        // Lock acquired exactly once; the second cycle skips re-acquisition.
        verify(connection, times(1)).prepareStatement("SELECT pg_try_advisory_lock(?)");
    }

    @Test
    void leaderStatusGauge_registered() {
        newElection();

        assertThat(meterRegistry.find("cce.scheduler.leader.status").gauge()).isNotNull();
    }
}
