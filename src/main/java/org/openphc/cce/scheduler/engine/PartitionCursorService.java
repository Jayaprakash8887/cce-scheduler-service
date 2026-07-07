package org.openphc.cce.scheduler.engine;

import org.openphc.cce.scheduler.domain.model.PartitionCursor;
import org.openphc.cce.scheduler.domain.repository.PartitionCursorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Reads and advances the per-partition scan watermark.
 *
 * <p>The watermark is the exclusive lower bound of the scan window: the scanner
 * considers only rows whose state-specific threshold is strictly greater than
 * it. {@link SchedulerLoop} advances it to the largest emitted threshold after a
 * fully successful publish cycle.
 */
@Service
public class PartitionCursorService {

    /** Watermark used when no cursor row exists yet (first scan) or when the
     *  feature is disabled — scans everything due up to now, i.e. today's behavior. */
    public static final OffsetDateTime BEGINNING =
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private final PartitionCursorRepository repository;

    public PartitionCursorService(PartitionCursorRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public OffsetDateTime readWatermark(int partitionIndex) {
        return repository.findById(partitionIndex)
                .map(PartitionCursor::getWatermark)
                .orElse(BEGINNING);
    }

    @Transactional
    public void advanceWatermark(int partitionIndex, OffsetDateTime watermark) {
        repository.upsertWatermark(partitionIndex, watermark);
    }
}
