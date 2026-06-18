package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.SchedulerNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Repository
public interface SchedulerNodeRepository extends JpaRepository<SchedulerNode, String> {

    /** Count pods whose heartbeat is still fresh (after the given threshold). */
    long countByLastHeartbeatAfter(OffsetDateTime threshold);

    /** Prune pods that have stopped heartbeating (dead/evicted). */
    @Transactional
    void deleteByLastHeartbeatBefore(OffsetDateTime threshold);
}
