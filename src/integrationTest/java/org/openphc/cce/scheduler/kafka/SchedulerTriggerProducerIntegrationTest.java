package org.openphc.cce.scheduler.kafka;

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
import org.openphc.cce.scheduler.domain.model.enums.TransitionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "cce.scheduler.leader-retry-interval=60000"
})
@Testcontainers
class SchedulerTriggerProducerIntegrationTest {

    private static final String TOPIC = "cce.scheduler.triggers";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("ccedb")
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
    private SchedulerTriggerProducer producer;

    private KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void publish_messageIsConsumedWithCorrectKey() throws Exception {
        UUID protocolId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        SchedulerTriggerMessage message = new SchedulerTriggerMessage(
                stepId, TransitionType.PENDING_TO_DUE,
                OffsetDateTime.now(ZoneOffset.UTC),
                "sched-PENDING_TO_DUE-" + stepId.toString().substring(0, 8));

        producer.publish(protocolId, message).get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> record = pollSingleRecord();
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(protocolId.toString());

        JsonNode value = objectMapper.readTree(record.value());
        assertThat(value.get("stepInstanceId").asText()).isEqualTo(stepId.toString());
        assertThat(value.get("transitionType").asText()).isEqualTo("PENDING_TO_DUE");
        assertThat(value.get("correlationId").asText()).startsWith("sched-PENDING_TO_DUE-");
    }

    @Test
    void publish_allTransitionTypes_succeed() throws Exception {
        for (TransitionType type : TransitionType.values()) {
            UUID protocolId = UUID.randomUUID();
            UUID stepId = UUID.randomUUID();
            SchedulerTriggerMessage message = new SchedulerTriggerMessage(
                    stepId, type,
                    OffsetDateTime.now(ZoneOffset.UTC),
                    "sched-" + type.name() + "-" + stepId.toString().substring(0, 8));

            producer.publish(protocolId, message).get(10, TimeUnit.SECONDS);
        }

        // Consume all messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(TransitionType.values().length);
    }

    @Test
    void publish_multipleMessages_sameProtocol_sameKey() throws Exception {
        UUID protocolId = UUID.randomUUID();

        for (int i = 0; i < 3; i++) {
            UUID stepId = UUID.randomUUID();
            SchedulerTriggerMessage message = new SchedulerTriggerMessage(
                    stepId, TransitionType.PENDING_TO_DUE,
                    OffsetDateTime.now(ZoneOffset.UTC),
                    "sched-PENDING_TO_DUE-" + stepId.toString().substring(0, 8));
            producer.publish(protocolId, message).get(10, TimeUnit.SECONDS);
        }

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(3);
        for (ConsumerRecord<String, String> record : records) {
            assertThat(record.key()).isEqualTo(protocolId.toString());
        }
    }

    private ConsumerRecord<String, String> pollSingleRecord() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isGreaterThanOrEqualTo(1);
        return records.iterator().next();
    }
}
