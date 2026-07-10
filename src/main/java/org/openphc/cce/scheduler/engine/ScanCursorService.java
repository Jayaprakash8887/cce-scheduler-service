package org.openphc.cce.scheduler.engine;

import org.openphc.cce.scheduler.domain.repository.ScanCursorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Reads and advances the single-row scan cursor — a keyset/seek cursor over the
 * composite key {@code (threshold, step id)}.
 *
 * <p>The cursor is the exclusive lower bound of the scan window: the scanner considers
 * only rows that sort strictly after it in {@code (threshold, id)} order. {@link
 * SchedulerLoop} advances it to the last emitted {@code (threshold, id)} after a fully
 * successful publish cycle. The id component breaks threshold ties so a cohort of steps
 * sharing an identical threshold drains across cycles rather than being truncated.
 */
@Service
public class ScanCursorService {

    /** Timestamp component used when no cursor row exists yet — first scan sees the full backlog up to now. */
    public static final OffsetDateTime BEGINNING =
            OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    /** Id component of the initial cursor — the minimum UUID, so every real id sorts after it. */
    public static final UUID BEGINNING_ID =
            new UUID(0L, 0L);

    /** Fixed key of the single cursor row. */
    private static final int CURSOR_ID = 0;

    /** Composite scan cursor: emit rows sorting strictly after ({@code timestamp}, {@code id}). */
    public record Watermark(OffsetDateTime timestamp, UUID id) {}

    private static final Watermark BEGINNING_WATERMARK = new Watermark(BEGINNING, BEGINNING_ID);

    private final ScanCursorRepository repository;

    public ScanCursorService(ScanCursorRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public Watermark readWatermark() {
        return repository.findById(CURSOR_ID)
                .map(c -> new Watermark(c.getWatermark(), c.getWatermarkId()))
                .orElse(BEGINNING_WATERMARK);
    }

    @Transactional
    public void advanceWatermark(OffsetDateTime watermark, UUID watermarkId) {
        repository.upsertWatermark(CURSOR_ID, watermark, watermarkId);
    }
}
