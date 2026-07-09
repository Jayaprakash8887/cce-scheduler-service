package org.openphc.cce.scheduler.leader;

import io.micrometer.core.instrument.MeterRegistry;
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
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single-leader election via one PostgreSQL session-level advisory lock.
 *
 * <p>Exactly one instance can hold the lock at a time, so only that instance scans
 * and publishes; every other instance stays on standby. The lock is held for the life
 * of a dedicated JDBC connection (outside the HikariCP pool), so it auto-releases if
 * the instance crashes or its connection drops — a surviving standby then acquires it
 * on its next retry. This guarantees no two instances process records simultaneously
 * with zero external coordination (no ZooKeeper/etcd).
 */
@Component
@Slf4j
public class LeaderElection {

    /** Single logical partition — the lease row / metric this instance heartbeats. */
    private static final int SINGLE_PARTITION = 0;

    private final SchedulerProperties properties;
    private final SchedulerLeaseRepository leaseRepository;
    private final DataSource dataSource;
    private final String leaderId;

    private volatile Connection dedicatedConnection;
    /** Whether we hold the advisory lock on the CURRENT dedicated connection.
     *  Reset whenever the connection is (re)established or dropped, so a silently
     *  dropped connection can never leave us believing we are still leader. */
    private boolean holdsLock = false;
    private volatile LeaderState state = new LeaderState(false, null, null);
    private final ReentrantLock connectionLock = new ReentrantLock();

    public record LeaderState(boolean leader, OffsetDateTime lastHeartbeat, OffsetDateTime leaseExpiresAt) {}

    public LeaderElection(
            SchedulerProperties properties,
            SchedulerLeaseRepository leaseRepository,
            DataSource dataSource,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.leaseRepository = leaseRepository;
        this.dataSource = dataSource;
        this.leaderId = UUID.randomUUID().toString();

        // leader.status gauge: 1 = leader, 0 = standby.
        meterRegistry.gauge("cce.scheduler.leader.status", this, l -> l.isLeader() ? 1.0 : 0.0);

        log.info("LeaderElection initialized — leaderId={}", leaderId);
    }

    @Scheduled(fixedDelayString = "${cce.scheduler.leader-retry-interval}")
    public void tryAcquireLeadership() {
        connectionLock.lock();
        try {
            ensureDedicatedConnection();
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

            // Acquire only if we don't already hold it on this connection (session-level
            // advisory locks persist for the connection's life, so no re-acquire needed).
            if (!holdsLock) {
                holdsLock = tryAcquireLock();
            }
            boolean leader = holdsLock;

            MDC.put("leaderStatus", leader ? "leader" : "standby");
            try {
                if (leader) {
                    OffsetDateTime expiresAt = now.plusSeconds(properties.getLeaseDurationSeconds());
                    this.state = new LeaderState(true, now, expiresAt);
                    updateHeartbeat(now, expiresAt);
                    log.debug("Leader {} holds the scheduler lock", leaderId);
                } else {
                    this.state = new LeaderState(false, state.lastHeartbeat(), state.leaseExpiresAt());
                    log.debug("Standby {} — scheduler lock held elsewhere", leaderId);
                }
            } finally {
                MDC.remove("leaderStatus");
            }
        } catch (SQLException e) {
            log.warn("Leader election cycle failed — resetting connection", e);
            closeConnection();
            // Closing the connection drops the advisory lock held on it.
            this.state = new LeaderState(false, null, null);
        } finally {
            connectionLock.unlock();
        }
    }

    private boolean tryAcquireLock() throws SQLException {
        try (PreparedStatement stmt = dedicatedConnection.prepareStatement(
                "SELECT pg_try_advisory_lock(?)")) {
            stmt.setLong(1, properties.getAdvisoryLockKey());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void releaseLock() throws SQLException {
        try (PreparedStatement stmt = dedicatedConnection.prepareStatement(
                "SELECT pg_advisory_unlock(?)")) {
            stmt.setLong(1, properties.getAdvisoryLockKey());
            stmt.executeQuery();
        }
    }

    private void updateHeartbeat(OffsetDateTime now, OffsetDateTime expiresAt) {
        SchedulerLease lease = leaseRepository.findByPartitionIndex(SINGLE_PARTITION)
                .orElseGet(() -> {
                    SchedulerLease newLease = new SchedulerLease();
                    newLease.setPartitionIndex(SINGLE_PARTITION);
                    return newLease;
                });
        lease.setLeaderId(leaderId);
        lease.setLastHeartbeat(now);
        lease.setLeaseExpiresAt(expiresAt);
        leaseRepository.save(lease);
    }

    private void ensureDedicatedConnection() throws SQLException {
        if (dedicatedConnection == null || dedicatedConnection.isClosed()) {
            dedicatedConnection = dataSource.getConnection();
            dedicatedConnection.setAutoCommit(true);
            holdsLock = false; // a fresh connection holds no advisory lock yet
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
        holdsLock = false;
    }

    @PreDestroy
    public void shutdown() {
        connectionLock.lock();
        try {
            if (holdsLock && dedicatedConnection != null && !dedicatedConnection.isClosed()) {
                releaseLock();
                log.info("Leader {} released the scheduler lock", leaderId);
            }
            closeConnection();
            this.state = new LeaderState(false, null, null);
        } catch (SQLException e) {
            log.warn("Error during leader election shutdown", e);
        } finally {
            connectionLock.unlock();
        }
    }

    public boolean isLeader() {
        return state.leader();
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
}
