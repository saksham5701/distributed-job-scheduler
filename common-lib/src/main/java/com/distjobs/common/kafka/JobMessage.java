package com.distjobs.common.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * Payload published to Kafka. Key = jobId for partition affinity (same job orders on one partition).
 */
public record JobMessage(
        @JsonProperty("jobId") UUID jobId,
        @JsonProperty("payload") String payload,
        @JsonProperty("retryCount") int retryCount,
        @JsonProperty("priority") int priority) {
}
