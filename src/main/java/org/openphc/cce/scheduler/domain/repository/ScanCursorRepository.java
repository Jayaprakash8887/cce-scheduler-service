package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.ScanCursor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface ScanCursorRepository extends JpaRepository<ScanCursor, Integer> {

    /**
     * Upsert the single-row composite watermark {@code (watermark, watermark_id)}. The
     * {@code WHERE} guard makes the write monotonic in lexical {@code (timestamp, id)}
     * order — a stale or clock-skewed value from a new leader after failover can never
     * move the cursor backwards.
     */
    @Modifying
    @Query(value = """
            INSERT INTO scheduler_scan_cursor (id, watermark, watermark_id, updated_at)
            VALUES (:id, :watermark, :watermarkId, now())
            ON CONFLICT (id)
            DO UPDATE SET watermark = EXCLUDED.watermark,
                          watermark_id = EXCLUDED.watermark_id,
                          updated_at = now()
            WHERE ROW(EXCLUDED.watermark, EXCLUDED.watermark_id)
                > ROW(scheduler_scan_cursor.watermark, scheduler_scan_cursor.watermark_id)
            """, nativeQuery = true)
    void upsertWatermark(@Param("id") int id,
                         @Param("watermark") OffsetDateTime watermark,
                         @Param("watermarkId") UUID watermarkId);
}
