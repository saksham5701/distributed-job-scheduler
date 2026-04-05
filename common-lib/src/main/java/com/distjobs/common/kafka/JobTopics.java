package com.distjobs.common.kafka;

/**
 * Topic names shared by scheduler (producer) and worker (consumer).
 * DLQ holds jobs that exceeded max retries for inspection/replay.
 */
public final class JobTopics {

    public static final String JOBS = "jobs-topic";
    public static final String DLQ = "jobs-dlq";

    private JobTopics() {
    }
}
