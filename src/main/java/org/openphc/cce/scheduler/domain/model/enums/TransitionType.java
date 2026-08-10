package org.openphc.cce.scheduler.domain.model.enums;

public enum TransitionType {
    PENDING_TO_DUE,
    DUE_TO_MISSED;

    /**
     * Determines the transition type based on the current step state.
     */
    public static TransitionType fromState(StepState state) {
        return switch (state) {
            case PENDING -> PENDING_TO_DUE;
            case DUE -> DUE_TO_MISSED;
            default -> throw new IllegalArgumentException(
                    "No transition defined for terminal state: " + state);
        };
    }
}
