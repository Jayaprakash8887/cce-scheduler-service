package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.PartitionCursor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface PartitionCursorRepository extends JpaRepository<PartitionCursor, Integer> {

    /**
     * Upsert the composite watermark {@code (watermark, watermark_id)} for a
     * partition. The {@code WHERE} guard makes the write monotonic in lexical
     * {@code (timestamp, id)} order — a stale or clock-skewed value from a new
     * owner after failover can never move the cursor backwards.
     */
    @Modifying
    @Query(value = """
            INSERT INTO scheduler_partition_cursor (partition_index, watermark, watermark_id, updated_at)
            VALUES (:partitionIndex, :watermark, :watermarkId, now())
            ON CONFLICT (partition_index)
            DO UPDATE SET watermark = EXCLUDED.watermark,
                          watermark_id = EXCLUDED.watermark_id,
                          updated_at = now()
            WHERE ROW(EXCLUDED.watermark, EXCLUDED.watermark_id)
                > ROW(scheduler_partition_cursor.watermark, scheduler_partition_cursor.watermark_id)
            """, nativeQuery = true)
    void upsertWatermark(@Param("partitionIndex") int partitionIndex,
                         @Param("watermark") OffsetDateTime watermark,
                         @Param("watermarkId") UUID watermarkId);
}
