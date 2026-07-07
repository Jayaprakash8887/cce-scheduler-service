package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.StepInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface StepInstanceRepository extends JpaRepository<StepInstance, UUID> {

    @Query(value = """
            SELECT s.* FROM step_instance s
            WHERE (
                (s.state = 'PENDING' AND s.due_date > :watermark AND s.due_date <= :now)
                OR (s.state = 'DUE' AND s.overdue_date > :watermark AND s.overdue_date <= :now)
                OR (s.state = 'OVERDUE' AND s.missed_date > :watermark AND s.missed_date <= :now)
            )
            AND MOD(ABS(('x' || SUBSTR(MD5(s.protocol_instance_id::text), 1, 8))::bit(32)::int), :totalPartitions) = :partitionIndex
            ORDER BY (CASE s.state
                WHEN 'PENDING' THEN s.due_date
                WHEN 'DUE' THEN s.overdue_date
                WHEN 'OVERDUE' THEN s.missed_date
            END) ASC
            LIMIT :batchSize
            """, nativeQuery = true)
    List<StepInstance> findDueSteps(
            @Param("now") OffsetDateTime now,
            @Param("watermark") OffsetDateTime watermark,
            @Param("partitionIndex") int partitionIndex,
            @Param("totalPartitions") int totalPartitions,
            @Param("batchSize") int batchSize
    );
}
