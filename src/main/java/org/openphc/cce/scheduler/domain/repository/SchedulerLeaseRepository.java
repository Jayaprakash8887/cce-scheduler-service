package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Repository
public interface SchedulerLeaseRepository extends JpaRepository<SchedulerLease, Integer> {

    /** Upsert the single leader-heartbeat row (id = 0). */
    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO scheduler_lease (id, leader_id, last_heartbeat, lease_expires_at)
            VALUES (0, :leaderId, :lastHeartbeat, :leaseExpiresAt)
            ON CONFLICT (id) DO UPDATE SET leader_id = EXCLUDED.leader_id,
                                           last_heartbeat = EXCLUDED.last_heartbeat,
                                           lease_expires_at = EXCLUDED.lease_expires_at
            """, nativeQuery = true)
    void upsertLease(@Param("leaderId") String leaderId,
                     @Param("lastHeartbeat") OffsetDateTime lastHeartbeat,
                     @Param("leaseExpiresAt") OffsetDateTime leaseExpiresAt);
}
