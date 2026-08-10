package org.openphc.cce.scheduler.domain.model.enums;

public enum StepState {
    PENDING,
    DUE,
    /**
     * Legacy state — no longer part of the lifecycle. The Scheduler never scans it and never
     * transitions a step into it; the constant is retained only so rows written before the
     * DUE → MISSED change still map when loaded.
     */
    OVERDUE,
    MISSED,
    COMPLETED,
    SKIPPED
}
