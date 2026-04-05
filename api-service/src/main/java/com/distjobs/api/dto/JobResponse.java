package com.distjobs.api.dto;

import com.distjobs.common.model.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        JobStatus status,
        Instant scheduleTime,
        int retryCount,
        String payload,
        int priority,
        Instant createdAt,
        Instant updatedAt) {
}
