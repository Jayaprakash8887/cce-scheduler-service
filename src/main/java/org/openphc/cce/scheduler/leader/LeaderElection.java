package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.scheduler.config.SchedulerProperties;
import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.openphc.cce.scheduler.domain.model.SchedulerNode;
import org.openphc.cce.scheduler.domain.repository.SchedulerLeaseRepository;
import org.openphc.cce.scheduler.domain.repository.SchedulerNodeRepository;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Slf4j
public class LeaderElection {

    private final SchedulerProperties properties;
    private final SchedulerLeaseRepository leaseRepository;
    private final SchedulerNodeRepository nodeRepository;
    private final DataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final String leaderId;

    private volatile Connection dedicatedConnection;
    private volatile LeaderState state = new LeaderState(Collections.emptyList(), null, null);
    private final ReentrantLock connectionLock = new ReentrantLock();

    /** Partitions whose advisory lock this pod currently holds. Guarded by {@link #connectionLock}. */
    private final Set<Integer> heldPartitions = new TreeSet<>();

    public record LeaderState(List<Integer> ownedPartitions, OffsetDateTime lastHeartbeat, OffsetDateTime leaseExpiresAt) {}

    public LeaderElection(
            SchedulerProperties properties,
            SchedulerLeaseRepository leaseRepository,
            SchedulerNodeRepository nodeRepository,
            DataSource dataSource,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.leaseRepository = leaseRepository;
        this.nodeRepository = nodeRepository;
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
            int totalPartitions = properties.getTotalPartitions();
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

            // Make this pod visible to peers (standbys included), then derive how
            // many partitions we're entitled to given the current cluster size.
            registerNodeHeartbeat(now, heldPartitions.size());
            int activeNodes = countActiveNodes(now);
            int fairShare = fairShare(totalPartitions, activeNodes);

            // Shed any surplus we grabbed while alone so newly-joined pods can take it.
            releaseDownTo(fairShare);

            // Acquire free partitions, but never beyond our fair share.
            for (int i = 0; i < totalPartitions && heldPartitions.size() < fairShare; i++) {
                if (heldPartitions.contains(i)) {
                    continue;
                }
                if (tryAcquireLock(i)) {
                    heldPartitions.add(i);
                }
                // Micro-delay between consecutive lock attempts for fair distribution
                if (i < totalPartitions - 1 && properties.getLockAcquireDelayMs() > 0) {
                    sleepJitter(properties.getLockAcquireDelayMs());
                }
            }

            List<Integer> acquired = new ArrayList<>(heldPartitions);
            List<Integer> ownedList = Collections.unmodifiableList(acquired);

            MDC.put("leaderStatus", acquired.isEmpty() ? "standby" : "leader");
            MDC.put("ownedPartitions", acquired.toString());
            MDC.put("totalPartitions", String.valueOf(totalPartitions));

            try {
                if (!acquired.isEmpty()) {
                    OffsetDateTime expiresAt = now.plusSeconds(properties.getLeaseDurationSeconds());
                    this.state = new LeaderState(ownedList, now, expiresAt);
                    updateHeartbeat(acquired, now, expiresAt);
                    log.info("Leader {} owns partitions {} (fairShare={}, activeNodes={})",
                            leaderId, acquired, fairShare, activeNodes);
                } else {
                    this.state = new LeaderState(ownedList, state.lastHeartbeat(), state.leaseExpiresAt());
                    log.debug("Standby {} — no partitions owned (fairShare={}, activeNodes={})",
                            leaderId, fairShare, activeNodes);
                }
            } finally {
                MDC.remove("leaderStatus");
                MDC.remove("ownedPartitions");
                MDC.remove("totalPartitions");
            }
        } catch (SQLException e) {
            log.warn("Leader election cycle failed — resetting connection", e);
            closeConnection();
            // Closing the connection drops every advisory lock held on it.
            heldPartitions.clear();
            this.state = new LeaderState(Collections.emptyList(), null, null);
        } finally {
            connectionLock.unlock();
        }
    }

    /**
     * Partitions a single pod is entitled to: {@code ceil(totalPartitions / activeNodes)}.
     * The ceiling guarantees every partition is claimable (no orphans when the split is
     * uneven); the trade-off is that with an uneven split the last pod may end up a
     * standby rather than each pod differing by exactly one.
     */
    static int fairShare(int totalPartitions, int activeNodes) {
        if (activeNodes <= 1) {
            return totalPartitions;
        }
        return (int) Math.ceil((double) totalPartitions / activeNodes);
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

    /** Release highest-indexed held partitions until we hold no more than {@code fairShare}. */
    private void releaseDownTo(int fairShare) throws SQLException {
        while (heldPartitions.size() > fairShare) {
            int victim = Collections.max(heldPartitions);
            releaseLock(victim);
            heldPartitions.remove(victim);
            log.info("Leader {} released partition {} to rebalance (fairShare={})",
                    leaderId, victim, fairShare);
        }
    }

    private void releaseLock(int partitionIndex) throws SQLException {
        long lockKey = properties.getAdvisoryLockKey() + partitionIndex;
        try (PreparedStatement stmt = dedicatedConnection.prepareStatement(
                "SELECT pg_advisory_unlock(?)")) {
            stmt.setLong(1, lockKey);
            stmt.executeQuery();
        }
    }

    private void registerNodeHeartbeat(OffsetDateTime now, int ownedCount) {
        SchedulerNode node = nodeRepository.findById(leaderId)
                .orElseGet(() -> {
                    SchedulerNode newNode = new SchedulerNode();
                    newNode.setNodeId(leaderId);
                    return newNode;
                });
        node.setLastHeartbeat(now);
        node.setOwnedCount(ownedCount);
        nodeRepository.save(node);
    }

    private int countActiveNodes(OffsetDateTime now) {
        OffsetDateTime threshold = now.minusSeconds(properties.getLeaseDurationSeconds());
        nodeRepository.deleteByLastHeartbeatBefore(threshold);
        long count = nodeRepository.countByLastHeartbeatAfter(threshold);
        // We registered ourselves above, so the cluster is at least size 1.
        return (int) Math.max(1L, count);
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
                // Release only the advisory locks this pod actually holds.
                for (int partitionIndex : heldPartitions) {
                    try {
                        releaseLock(partitionIndex);
                    } catch (SQLException e) {
                        log.debug("Error releasing lock for partition {}", partitionIndex, e);
                    }
                }
                log.info("Leader {} released advisory locks {}", leaderId, heldPartitions);
            }
            heldPartitions.clear();
            closeConnection();
            this.state = new LeaderState(Collections.emptyList(), null, null);
        } catch (SQLException e) {
            log.warn("Error during leader election shutdown", e);
        } finally {
            // Deregister so peers stop counting us toward the cluster size immediately.
            try {
                nodeRepository.deleteById(leaderId);
            } catch (RuntimeException e) {
                log.debug("Error deregistering node {}", leaderId, e);
            }
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
