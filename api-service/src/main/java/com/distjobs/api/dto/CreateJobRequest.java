package com.distjobs.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record CreateJobRequest(
        /** If null, job is eligible immediately. */
        Instant scheduleTime,
        @NotBlank String payload,
        int priority) {

    public CreateJobRequest {
        if (priority < 0) {
            priority = 0;
        }
    }
}
