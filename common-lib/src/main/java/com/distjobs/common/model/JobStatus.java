package com.distjobs.common.model;

/**
 * Job lifecycle: PENDING → QUEUED (scheduler) → RUNNING (worker) → COMPLETED | FAILED.
 * Retries move back to PENDING with a future {@code scheduleTime}.
 */
public enum JobStatus {
    PENDING,
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED
}
