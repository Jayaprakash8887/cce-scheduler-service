package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.PartitionCursor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;

@Repository
public interface PartitionCursorRepository extends JpaRepository<PartitionCursor, Integer> {

    /**
     * Upsert the watermark for a partition. {@code GREATEST} makes the write
     * monotonic — a stale or clock-skewed value from a new owner after failover
     * can never move the watermark backwards.
     */
    @Modifying
    @Query(value = """
            INSERT INTO scheduler_partition_cursor (partition_index, watermark, updated_at)
            VALUES (:partitionIndex, :watermark, now())
            ON CONFLICT (partition_index)
            DO UPDATE SET watermark = GREATEST(scheduler_partition_cursor.watermark, EXCLUDED.watermark),
                          updated_at = now()
            """, nativeQuery = true)
    void upsertWatermark(@Param("partitionIndex") int partitionIndex,
                         @Param("watermark") OffsetDateTime watermark);
}
