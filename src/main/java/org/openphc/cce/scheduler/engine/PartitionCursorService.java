package org.openphc.cce.scheduler.engine;

import org.openphc.cce.scheduler.domain.repository.PartitionCursorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Reads and advances the per-partition scan watermark, a keyset/seek cursor over
 * the composite key {@code (threshold, step id)}.
 *
 * <p>The cursor is the exclusive lower bound of the scan window: the scanner
 * considers only rows that sort strictly after it in {@code (threshold, id)}
 * order. {@link SchedulerLoop} advances it to the last emitted {@code (threshold,
 * id)} after a fully successful publish cycle. The id component breaks threshold
 * ties so a cohort of steps sharing an identical threshold drains across cycles
 * rather than being truncated at the batch boundary.
 */
@Service
public class PartitionCursorService {

    /** Timestamp component used when no cursor row exists yet (first scan) or when
     *  the feature is disabled — scans everything due up to now, i.e. today's behavior. */
    public static final OffsetDateTime BEGINNING =
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    /** Id component of the initial cursor — the minimum UUID, so every real id sorts after it. */
    public static final UUID BEGINNING_ID =
            new UUID(0L, 0L);

    /** Composite scan cursor: emit rows sorting strictly after ({@code timestamp}, {@code id}). */
    public record Watermark(OffsetDateTime timestamp, UUID id) {}

    private static final Watermark BEGINNING_WATERMARK = new Watermark(BEGINNING, BEGINNING_ID);

    private final PartitionCursorRepository repository;

    public PartitionCursorService(PartitionCursorRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public Watermark readWatermark(int partitionIndex) {
        return repository.findById(partitionIndex)
                .map(c -> new Watermark(c.getWatermark(), c.getWatermarkId()))
                .orElse(BEGINNING_WATERMARK);
    }

    @Transactional
    public void advanceWatermark(int partitionIndex, OffsetDateTime watermark, UUID watermarkId) {
        repository.upsertWatermark(partitionIndex, watermark, watermarkId);
    }
}
