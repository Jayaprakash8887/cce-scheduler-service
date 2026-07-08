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
 * Per-partition scan watermark, a keyset/seek cursor over the composite key
 * {@code (watermark, watermark_id)}. Read at the start of each scan to bound the
 * query's lower edge; advanced by {@code SchedulerLoop} to the last emitted
 * {@code (threshold, step id)} after a fully successful publish cycle. One row
 * per partition index.
 */
@Entity
@Table(name = "scheduler_partition_cursor")
@Getter
@Setter
@NoArgsConstructor
public class PartitionCursor {

    @Id
    @Column(name = "partition_index")
    private int partitionIndex;

    @Column(name = "watermark", nullable = false)
    private OffsetDateTime watermark;

    @Column(name = "watermark_id", nullable = false)
    private UUID watermarkId;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
}
