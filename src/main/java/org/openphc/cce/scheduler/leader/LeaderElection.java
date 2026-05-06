package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Slf4j
public class LeaderElection {

    private final SchedulerProperties properties;
    private final SchedulerLeaseRepository leaseRepository;
    private final DataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final String leaderId;

    private volatile Connection dedicatedConnection;
    private volatile LeaderState state = new LeaderState(Collections.emptyList(), null, null);
    private final ReentrantLock connectionLock = new ReentrantLock();

    public record LeaderState(List<Integer> ownedPartitions, OffsetDateTime lastHeartbeat, OffsetDateTime leaseExpiresAt) {}

    public LeaderElection(
            SchedulerProperties properties,
            SchedulerLeaseRepository leaseRepository,
            DataSource dataSource,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.leaseRepository = leaseRepository;
        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.leaderId = UUID.randomUUID().toString();

        // Register leader.status gauge per partition
        for (int i = 0; i < properties.getTotalPartitions(); i++) {
            final int partition = i;
            meterRegistry.gauge("cce.scheduler.leader.status",
                    Tags.of("partition", String.valueOf(partition)),
                    this,
                    leader -> leader.getOwnedPartitions().contains(partition) ? 1.0 : 0.0);
        }

        log.info("LeaderElection initialized — leaderId={}, totalPartitions={}, lockAcquireDelayMs={}",
                leaderId, properties.getTotalPartitions(), properties.getLockAcquireDelayMs());
    }

    @Scheduled(fixedDelayString = "${cce.scheduler.leader-retry-interval}")
    public void tryAcquirePartitions() {
        connectionLock.lock();
        try {
            ensureDedicatedConnection();
            List<Integer> acquired = new ArrayList<>();
            int totalPartitions = properties.getTotalPartitions();

            for (int i = 0; i < totalPartitions; i++) {
                if (tryAcquireLock(i)) {
                    acquired.add(i);
                }
                // Micro-delay between consecutive lock attempts for fair distribution
                if (i < totalPartitions - 1 && properties.getLockAcquireDelayMs() > 0) {
                    sleepJitter(properties.getLockAcquireDelayMs());
                }
            }

            List<Integer> ownedList = Collections.unmodifiableList(acquired);

            MDC.put("leaderStatus", acquired.isEmpty() ? "standby" : "leader");
            MDC.put("ownedPartitions", acquired.toString());
            MDC.put("totalPartitions", String.valueOf(totalPartitions));

            try {
                if (!acquired.isEmpty()) {
                    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
                    OffsetDateTime expiresAt = now.plusSeconds(properties.getLeaseDurationSeconds());
                    this.state = new LeaderState(ownedList, now, expiresAt);
                    updateHeartbeat(acquired, now, expiresAt);
                    log.info("Leader {} owns partitions: {}", leaderId, acquired);
                } else {
                    this.state = new LeaderState(ownedList, state.lastHeartbeat(), state.leaseExpiresAt());
                    log.debug("Standby {} — no partitions acquired", leaderId);
                }
            } finally {
                MDC.remove("leaderStatus");
                MDC.remove("ownedPartitions");
                MDC.remove("totalPartitions");
            }
        } catch (SQLException e) {
            log.warn("Leader election cycle failed — resetting connection", e);
            closeConnection();
            this.state = new LeaderState(Collections.emptyList(), null, null);
        } finally {
            connectionLock.unlock();
        }
    }

    private boolean tryAcquireLock(int partitionIndex) throws SQLException {
        long lockKey = properties.getAdvisoryLockKey() + partitionIndex;
        try (PreparedStatement stmt = dedicatedConnection.prepareStatement(
                "SELECT pg_try_advisory_lock(?)")) {
            stmt.setLong(1, lockKey);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void updateHeartbeat(List<Integer> partitions, OffsetDateTime now, OffsetDateTime expiresAt) {
        for (int partitionIndex : partitions) {
            SchedulerLease lease = leaseRepository.findByPartitionIndex(partitionIndex)
                    .orElseGet(() -> {
                        SchedulerLease newLease = new SchedulerLease();
                        newLease.setPartitionIndex(partitionIndex);
                        return newLease;
                    });
            lease.setLeaderId(leaderId);
            lease.setLastHeartbeat(now);
            lease.setLeaseExpiresAt(expiresAt);
            leaseRepository.save(lease);
        }
    }

    private void ensureDedicatedConnection() throws SQLException {
        if (dedicatedConnection == null || dedicatedConnection.isClosed()) {
            dedicatedConnection = dataSource.getConnection();
            dedicatedConnection.setAutoCommit(true);
            log.debug("Dedicated advisory-lock connection established");
        }
    }

    private void closeConnection() {
        if (dedicatedConnection != null) {
            try {
                dedicatedConnection.close();
            } catch (SQLException e) {
                log.debug("Error closing dedicated connection", e);
            }
            dedicatedConnection = null;
        }
    }

    void sleepJitter(int maxDelayMs) {
        try {
            int delay = ThreadLocalRandom.current().nextInt(0, maxDelayMs + 1);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            log.debug("Lock acquisition delay interrupted");
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        connectionLock.lock();
        try {
            if (dedicatedConnection != null && !dedicatedConnection.isClosed()) {
                // Release all held advisory locks
                int totalPartitions = properties.getTotalPartitions();
                for (int i = 0; i < totalPartitions; i++) {
                    long lockKey = properties.getAdvisoryLockKey() + i;
                    try (PreparedStatement stmt = dedicatedConnection.prepareStatement(
                            "SELECT pg_advisory_unlock(?)")) {
                        stmt.setLong(1, lockKey);
                        stmt.executeQuery();
                    } catch (SQLException e) {
                        log.debug("Error releasing lock for partition {}", i, e);
                    }
                }
                log.info("Leader {} released all advisory locks", leaderId);
            }
            closeConnection();
            this.state = new LeaderState(Collections.emptyList(), null, null);
        } catch (SQLException e) {
            log.warn("Error during leader election shutdown", e);
        } finally {
            connectionLock.unlock();
        }
    }

    public List<Integer> getOwnedPartitions() {
        return state.ownedPartitions();
    }

    public boolean isLeader() {
        return !state.ownedPartitions().isEmpty();
    }

    public String getLeaderId() {
        return leaderId;
    }

    public OffsetDateTime getLastHeartbeat() {
        return state.lastHeartbeat();
    }

    public OffsetDateTime getLeaseExpiresAt() {
        return state.leaseExpiresAt();
    }

    public int getTotalPartitions() {
        return properties.getTotalPartitions();
    }
}
