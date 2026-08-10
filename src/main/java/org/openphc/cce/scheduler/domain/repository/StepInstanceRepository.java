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
                (s.state = 'PENDING' AND s.due_date <= :now
                    AND (s.due_date > :watermark OR (s.due_date = :watermark AND s.id > :watermarkId)))
                OR (s.state = 'DUE' AND s.missed_date <= :now
                    AND (s.missed_date > :watermark OR (s.missed_date = :watermark AND s.id > :watermarkId)))
            )
            ORDER BY (CASE s.state
                WHEN 'PENDING' THEN s.due_date
                WHEN 'DUE' THEN s.missed_date
            END) ASC, s.id ASC
            LIMIT :batchSize
            """, nativeQuery = true)
    List<StepInstance> findDueSteps(
            @Param("now") OffsetDateTime now,
            @Param("watermark") OffsetDateTime watermark,
            @Param("watermarkId") UUID watermarkId,
            @Param("batchSize") int batchSize
    );
}
