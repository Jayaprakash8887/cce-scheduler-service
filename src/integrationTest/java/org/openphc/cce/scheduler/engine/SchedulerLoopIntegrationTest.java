package org.openphc.cce.scheduler.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openphc.cce.scheduler.leader.LeaderElection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "cce.scheduler.scan-interval=60000",
        "cce.scheduler.leader-retry-interval=60000",
        "cce.scheduler.total-partitions=1",
        "cce.scheduler.lock-acquire-delay-ms=0"
})
@Testcontainers
class SchedulerLoopIntegrationTest {

    private static final String TOPIC = "cce.scheduler.triggers";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("cce_collector")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("init-step-instance.sql");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> "false");
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("cce.kafka.topics.scheduler-triggers", () -> TOPIC);
    }

    @Autowired
    private SchedulerLoop schedulerLoop;

    @Autowired
    private LeaderElection leaderElection;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-loop-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        jdbcTemplate.execute("DELETE FROM step_instance");

        // Ensure this instance is leader
        leaderElection.tryAcquirePartitions();
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void fullCycle_pendingStep_publishesPendingToDue() throws Exception {
        UUID protocolId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        insertStep(stepId, protocolId, "PENDING",
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null, null);

        schedulerLoop.executeCycle();

        ConsumerRecord<String, String> record = pollSingleRecord();
        assertThat(record.key()).isEqualTo(protocolId.toString());

        JsonNode value = objectMapper.readTree(record.value());
        assertThat(value.get("stepInstanceId").asText()).isEqualTo(stepId.toString());
        assertThat(value.get("transitionType").asText()).isEqualTo("PENDING_TO_DUE");
        assertThat(value.get("correlationId").asText()).startsWith("sched-PENDING_TO_DUE-");
    }

    @Test
    void fullCycle_dueStep_publishesDueToOverdue() throws Exception {
        UUID protocolId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        insertStep(stepId, protocolId, "DUE",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1),
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1), null);

        schedulerLoop.executeCycle();

        ConsumerRecord<String, String> record = pollSingleRecord();
        JsonNode value = objectMapper.readTree(record.value());
        assertThat(value.get("transitionType").asText()).isEqualTo("DUE_TO_OVERDUE");
    }

    @Test
    void fullCycle_overdueStep_publishesOverdueToMissed() throws Exception {
        UUID protocolId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        insertStep(stepId, protocolId, "OVERDUE",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(2),
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1),
                OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

        schedulerLoop.executeCycle();

        ConsumerRecord<String, String> record = pollSingleRecord();
        JsonNode value = objectMapper.readTree(record.value());
        assertThat(value.get("transitionType").asText()).isEqualTo("OVERDUE_TO_MISSED");
    }

    @Test
    void fullCycle_completedStep_neverPublished() {
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "COMPLETED",
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(1), null, null);

        schedulerLoop.executeCycle();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
        assertThat(records.count()).isEqualTo(0);
    }

    @Test
    void fullCycle_emptyTable_noMessages() {
        schedulerLoop.executeCycle();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
        assertThat(records.count()).isEqualTo(0);
    }

    @Test
    void fullCycle_multipleSteps_allPublished() {
        OffsetDateTime pastDue = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);
        insertStep(UUID.randomUUID(), UUID.randomUUID(), "PENDING", pastDue, null, null);

        schedulerLoop.executeCycle();

        List<ConsumerRecord<String, String>> records = pollAllRecords(3);
        assertThat(records).hasSize(3);
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

    private ConsumerRecord<String, String> pollSingleRecord() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isGreaterThanOrEqualTo(1);
        return records.iterator().next();
    }

    private List<ConsumerRecord<String, String>> pollAllRecords(int expected) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (all.size() < expected && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            records.forEach(all::add);
        }
        return all;
    }
}
