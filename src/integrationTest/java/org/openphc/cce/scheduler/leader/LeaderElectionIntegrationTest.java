package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;
import org.openphc.cce.scheduler.domain.repository.SchedulerNodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        registry.add("cce.scheduler.lock-acquire-delay-ms", () -> "0");
    }

    @Autowired
    private SchedulerLeaseRepository leaseRepository;

    @Autowired
    private SchedulerNodeRepository nodeRepository;

    @Autowired
    private DataSource dataSource;

    private SchedulerProperties properties;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setAdvisoryLockKey(100001);
        properties.setLeaseDurationSeconds(30);
        properties.setLockAcquireDelayMs(0);
    }

    @AfterEach
    void cleanUp() {
        leaseRepository.deleteAll();
        nodeRepository.deleteAll();
    }

    @Test
    void singlePartition_acquireAndRelease() {
        properties.setTotalPartitions(1);

        LeaderElection election = createElection();
        try {
            election.tryAcquirePartitions();

            assertThat(election.isLeader()).isTrue();
            assertThat(election.getOwnedPartitions()).containsExactly(0);
            assertThat(election.getLastHeartbeat()).isNotNull();
            assertThat(election.getLeaseExpiresAt()).isNotNull();

            // Verify lease persisted
            Optional<SchedulerLease> lease = leaseRepository.findByPartitionIndex(0);
            assertThat(lease).isPresent();
            assertThat(lease.get().getLeaderId()).isEqualTo(election.getLeaderId());
        } finally {
            election.shutdown();
        }

        assertThat(election.getOwnedPartitions()).isEmpty();
        assertThat(election.isLeader()).isFalse();
    }

    @Test
    void multiPartition_singleInstance_ownsAll() {
        properties.setTotalPartitions(4);

        LeaderElection election = createElection();
        try {
            election.tryAcquirePartitions();

            assertThat(election.isLeader()).isTrue();
            assertThat(election.getOwnedPartitions()).containsExactly(0, 1, 2, 3);
        } finally {
            election.shutdown();
        }
    }

    @Test
    void multiPartition_twoInstances_rebalanceToFairShare() {
        properties.setTotalPartitions(4);

        SchedulerProperties props2 = new SchedulerProperties();
        props2.setTotalPartitions(4);
        props2.setAdvisoryLockKey(100001);
        props2.setLeaseDurationSeconds(30);
        props2.setLockAcquireDelayMs(0);

        LeaderElection election1 = createElection();
        LeaderElection election2 = createElection(props2);

        try {
            // Round 1: instance 1 boots alone and (correctly) grabs everything.
            election1.tryAcquirePartitions();
            assertThat(election1.getOwnedPartitions()).hasSize(4);

            // Instance 2 joins: it registers as a live node but every lock is still held.
            election2.tryAcquirePartitions();
            assertThat(election2.getOwnedPartitions()).isEmpty();

            // Round 2: instance 1 now sees 2 live nodes, fair share = 2, and sheds the surplus.
            election1.tryAcquirePartitions();
            assertThat(election1.getOwnedPartitions()).hasSize(2);

            // Instance 2 picks up the two freed partitions.
            election2.tryAcquirePartitions();
            assertThat(election2.getOwnedPartitions()).hasSize(2);

            // Together they cover all 4 with no overlap — real distribution, not all-or-nothing.
            assertThat(election1.getOwnedPartitions())
                    .doesNotContainAnyElementsOf(election2.getOwnedPartitions());
            assertThat(election1.getOwnedPartitions().size()
                    + election2.getOwnedPartitions().size()).isEqualTo(4);
        } finally {
            election1.shutdown();
            election2.shutdown();
        }
    }

    @Test
    void failover_survivingInstance_acquiresOrphanedPartitions() {
        properties.setTotalPartitions(4);

        SchedulerProperties props2 = new SchedulerProperties();
        props2.setTotalPartitions(4);
        props2.setAdvisoryLockKey(100001);
        props2.setLeaseDurationSeconds(30);
        props2.setLockAcquireDelayMs(0);

        LeaderElection election1 = createElection();
        LeaderElection election2 = createElection(props2);

        try {
            // Instance 1 acquires all
            election1.tryAcquirePartitions();
            assertThat(election1.getOwnedPartitions()).hasSize(4);

            // Simulate instance 1 crash (shutdown releases locks)
            election1.shutdown();

            // Instance 2 retries and picks up all orphaned partitions
            election2.tryAcquirePartitions();
            assertThat(election2.getOwnedPartitions()).containsExactly(0, 1, 2, 3);
        } finally {
            election1.shutdown();
            election2.shutdown();
        }
    }

    @Test
    void microDelay_producesMoreBalancedDistribution() throws InterruptedException {
        // With delay, concurrent instances can interleave lock acquisitions
        int totalPartitions = 8;
        properties.setTotalPartitions(totalPartitions);
        properties.setLockAcquireDelayMs(50);

        SchedulerProperties props2 = new SchedulerProperties();
        props2.setTotalPartitions(totalPartitions);
        props2.setAdvisoryLockKey(100001);
        props2.setLeaseDurationSeconds(30);
        props2.setLockAcquireDelayMs(50);

        LeaderElection election1 = createElection();
        LeaderElection election2 = createElection(props2);

        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            // Start both instances concurrently
            executor.submit(() -> {
                try { startLatch.await(); } catch (InterruptedException e) { return; }
                election1.tryAcquirePartitions();
            });
            executor.submit(() -> {
                try { startLatch.await(); } catch (InterruptedException e) { return; }
                election2.tryAcquirePartitions();
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // With concurrent start + jitter, partitions should be split (not all-or-nothing)
            List<Integer> all1 = election1.getOwnedPartitions();
            List<Integer> all2 = election2.getOwnedPartitions();

            // Together they own all partitions (no orphans)
            assertThat(all1.size() + all2.size()).isEqualTo(totalPartitions);

            // No overlapping partitions
            assertThat(all1).doesNotContainAnyElementsOf(all2);
        } finally {
            election1.shutdown();
            election2.shutdown();
        }
    }

    private LeaderElection createElection() {
        return createElection(properties);
    }

    private LeaderElection createElection(SchedulerProperties props) {
        return new LeaderElection(
                props,
                leaseRepository,
                nodeRepository,
                dataSource,
                new SimpleMeterRegistry());
    }
}
