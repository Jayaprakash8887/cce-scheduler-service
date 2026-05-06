package org.openphc.cce.scheduler.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.openphc.cce.scheduler.domain.repository.StepInstanceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "cce.scheduler.leader-retry-interval=60000"
})
@Testcontainers
class DueStepScannerIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("cce_collector")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("init-step-instance.sql");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> "false");
    }

    @Autowired
    private StepInstanceRepository stepInstanceRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DueStepScanner scanner;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setBatchSize(100);
        scanner = new DueStepScanner(stepInstanceRepository, properties);

        jdbcTemplate.execute("DELETE FROM step_instance");
    }

    @Test
    void scan_pendingStepWithPastDueDate_returnsPendingToDue() {
        UUID protocolId = UUID.randomUUID();
        insertStep(UUID.randomUUID(), protocolId, "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.PENDING_TO_DUE);
        assertThat(result.get(0).protocolInstanceId()).isEqualTo(protocolId);
    }

    @Test
    void scan_dueStepWithPastOverdueDate_returnsDueToOverdue() {
        UUID protocolId = UUID.randomUUID();
        insertStep(UUID.randomUUID(), protocolId, "DUE",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1),
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.DUE_TO_OVERDUE);
    }

    @Test
    void scan_overdueStepWithPastMissedDate_returnsOverdueToMissed() {
        UUID protocolId = UUID.randomUUID();
        insertStep(UUID.randomUUID(), protocolId, "OVERDUE",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(2),
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1),
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
    }

    @Test
    void scan_completedStep_neverReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "COMPLETED",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1), null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).isEmpty();
    }

    @Test
    void scan_skippedStep_neverReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "SKIPPED",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1), null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).isEmpty();
    }

    @Test
    void scan_pendingStepWithFutureDueDate_notReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).plusHours(1), null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).isEmpty();
    }

    @Test
    void scan_partitionFilter_onlyReturnsMatchingPartition() {
        // Insert steps with different protocol_instance_ids
        // With totalPartitions=2, they should be split across partitions
        UUID proto1 = UUID.randomUUID();
        UUID proto2 = UUID.randomUUID();
        OffsetDateTime pastDue = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);

        insertStep(UUID.randomUUID(), proto1, "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), proto2, "PENDING", pastDue, null, null);

        List<DueStep> partition0 = scanner.scan(0, 2);
        List<DueStep> partition1 = scanner.scan(1, 2);

        // Together both partitions cover all steps
        assertThat(partition0.size() + partition1.size()).isEqualTo(2);
        // No overlap
        List<UUID> ids0 = partition0.stream().map(DueStep::stepInstanceId).toList();
        List<UUID> ids1 = partition1.stream().map(DueStep::stepInstanceId).toList();
        assertThat(ids0).doesNotContainAnyElementsOf(ids1);
    }

    @Test
    void scan_singlePartition_returnsAllSteps() {
        OffsetDateTime pastDue = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(3);
    }

    @Test
    void scan_resultsOrderedByThresholdDate() {
        OffsetDateTime earliest = OffsetDateTime.now(ZoneOffset.UTC).minusHours(3);
        OffsetDateTime middle = OffsetDateTime.now(ZoneOffset.UTC).minusHours(2);
        OffsetDateTime latest = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);

        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", latest, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", earliest, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", middle, null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).thresholdDate()).isBeforeOrEqualTo(result.get(1).thresholdDate());
        assertThat(result.get(1).thresholdDate()).isBeforeOrEqualTo(result.get(2).thresholdDate());
    }

    @Test
    void scan_metadata_containsExpectedFields() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        List<DueStep> result = scanner.scan(0, 1);

        assertThat(result.get(0).metadata().has("actionId")).isTrue();
        assertThat(result.get(0).metadata().has("repeatIndex")).isTrue();
        assertThat(result.get(0).metadata().has("currentState")).isTrue();
        assertThat(result.get(0).metadata().get("currentState").asText()).isEqualTo("PENDING");
    }

    private void insertStep(UUID id, UUID protocolInstanceId, String state,
                            OffsetDateTime dueDate, OffsetDateTime overdueDate,
                            OffsetDateTime missedDate) {
        jdbcTemplate.update("""
                INSERT INTO step_instance (id, protocol_instance_id, action_id, repeat_index, 
                    state, due_date, overdue_date, missed_date, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                """,
                id, protocolInstanceId, "action-1", 0,
                state, dueDate, overdueDate, missedDate);
    }
}
