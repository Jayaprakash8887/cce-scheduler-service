package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration"
})
@Testcontainers
class LeaderElectionIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("ccedb")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("cce.scheduler.leader-retry-interval", () -> "60000");
    }

    @Autowired
    private SchedulerLeaseRepository leaseRepository;

    @Autowired
    private DataSource dataSource;

    private SchedulerProperties properties;

    @BeforeEach
    void setUp() {
        properties = newProperties();
    }

    @AfterEach
    void cleanUp() {
        leaseRepository.deleteAll();
    }

    @Test
    void acquireAndRelease() {
        LeaderElection election = createElection(properties);
        try {
            election.tryAcquireLeadership();

            assertThat(election.isLeader()).isTrue();
            assertThat(election.getLastHeartbeat()).isNotNull();
            assertThat(election.getLeaseExpiresAt()).isNotNull();

            Optional<SchedulerLease> lease = leaseRepository.findByPartitionIndex(0);
            assertThat(lease).isPresent();
            assertThat(lease.get().getLeaderId()).isEqualTo(election.getLeaderId());
        } finally {
            election.shutdown();
        }

        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void onlyOneInstanceIsLeaderAtATime() {
        LeaderElection election1 = createElection(properties);
        LeaderElection election2 = createElection(newProperties());
        try {
            election1.tryAcquireLeadership();
            assertThat(election1.isLeader()).isTrue();

            // The single advisory lock is held by election1, so election2 stays standby.
            election2.tryAcquireLeadership();
            assertThat(election2.isLeader()).isFalse();
        } finally {
            election1.shutdown();
            election2.shutdown();
        }
    }

    @Test
    void failover_survivingInstanceBecomesLeader() {
        LeaderElection election1 = createElection(properties);
        LeaderElection election2 = createElection(newProperties());
        try {
            election1.tryAcquireLeadership();
            assertThat(election1.isLeader()).isTrue();

            // election1 crashes/stops → its connection drops → the lock is released.
            election1.shutdown();

            election2.tryAcquireLeadership();
            assertThat(election2.isLeader()).isTrue();
        } finally {
            election1.shutdown();
            election2.shutdown();
        }
    }

    private SchedulerProperties newProperties() {
        SchedulerProperties props = new SchedulerProperties();
        props.setAdvisoryLockKey(100001);
        props.setLeaseDurationSeconds(30);
        return props;
    }

    private LeaderElection createElection(SchedulerProperties props) {
        return new LeaderElection(props, leaseRepository, dataSource, new SimpleMeterRegistry());
    }
}
