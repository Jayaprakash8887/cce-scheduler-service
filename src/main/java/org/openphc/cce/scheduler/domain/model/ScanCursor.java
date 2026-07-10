package org.openphc.cce.scheduler.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * The single-row scan cursor: a keyset/seek cursor over the composite key
 * {@code (watermark, watermark_id)}. Read at the start of each scan to bound the
 * query's lower edge; advanced by {@code SchedulerLoop} to the last emitted
 * {@code (threshold, step id)} after a fully successful publish cycle.
 */
@Entity
@Table(name = "scheduler_scan_cursor")
@Getter
@Setter
@NoArgsConstructor
public class ScanCursor {

    /** Fixed singleton key (always 0) — there is exactly one cursor row. */
    @Id
    @Column(name = "id")
    private int id;

    @Column(name = "watermark", nullable = false)
    private OffsetDateTime watermark;

    @Column(name = "watermark_id", nullable = false)
    private UUID watermarkId;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
}
