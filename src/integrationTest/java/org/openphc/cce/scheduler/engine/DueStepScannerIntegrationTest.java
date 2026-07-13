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
            .withDatabaseName("ccedb")
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
    private ScanCursorService scanCursor;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DueStepScanner scanner;

    @BeforeEach
    void setUp() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setBatchSize(100);
        scanner = new DueStepScanner(stepInstanceRepository, properties, scanCursor);

        jdbcTemplate.execute("DELETE FROM step_instance");
        jdbcTemplate.execute("DELETE FROM scheduler_scan_cursor");
    }

    @Test
    void scan_pendingStepWithPastDueDate_returnsPendingToDue() {
        UUID protocolId = UUID.randomUUID();
        insertStep(UUID.randomUUID(), protocolId, "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        List<DueStep> result = scanner.scan();

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

        List<DueStep> result = scanner.scan();

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

        List<DueStep> result = scanner.scan();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).transitionType()).isEqualTo(TransitionType.OVERDUE_TO_MISSED);
    }

    @Test
    void scan_completedStep_neverReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "COMPLETED",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1), null, null);

        List<DueStep> result = scanner.scan();

        assertThat(result).isEmpty();
    }

    @Test
    void scan_skippedStep_neverReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "SKIPPED",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1), null, null);

        List<DueStep> result = scanner.scan();

        assertThat(result).isEmpty();
    }

    @Test
    void scan_pendingStepWithFutureDueDate_notReturned() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).plusHours(1), null, null);

        List<DueStep> result = scanner.scan();

        assertThat(result).isEmpty();
    }

    @Test
    void scan_returnsAllDueSteps() {
        OffsetDateTime pastDue = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);

        List<DueStep> result = scanner.scan();

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

        List<DueStep> result = scanner.scan();

        assertThat(result).hasSize(3);
        assertThat(result.get(0).thresholdDate()).isBeforeOrEqualTo(result.get(1).thresholdDate());
        assertThat(result.get(1).thresholdDate()).isBeforeOrEqualTo(result.get(2).thresholdDate());
    }

    @Test
    void scan_identicalThresholds_drainAcrossCyclesViaKeyset() {
        // Three PENDING steps sharing the EXACT same due_date — a cohort larger
        // than the batch size. The keyset (threshold, id) cursor must drain them
        // across cycles without skipping any (gap "c" closed by the id tie-break).
        OffsetDateTime sameDue = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", sameDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", sameDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", sameDue, null, null);

        SchedulerProperties smallBatch = new SchedulerProperties();
        smallBatch.setBatchSize(2);
        DueStepScanner pagedScanner = new DueStepScanner(stepInstanceRepository, smallBatch, scanCursor);

        List<DueStep> firstCycle = pagedScanner.scan();
        assertThat(firstCycle).hasSize(2);
        DueStep last = firstCycle.get(firstCycle.size() - 1);
        scanCursor.advanceWatermark(last.thresholdDate(), last.stepInstanceId());

        List<DueStep> secondCycle = pagedScanner.scan();
        assertThat(secondCycle).hasSize(1); // the third step, not skipped

        List<UUID> firstIds = firstCycle.stream().map(DueStep::stepInstanceId).toList();
        assertThat(secondCycle.get(0).stepInstanceId()).isNotIn(firstIds);
    }

    @Test
    void scan_metadata_containsExpectedFields() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        List<DueStep> result = scanner.scan();

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
