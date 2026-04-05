package com.distjobs.worker.kafka;

import com.distjobs.common.kafka.JobMessage;
import com.distjobs.common.kafka.JobTopics;
import com.distjobs.common.model.Job;
import com.distjobs.common.model.JobStatus;
import com.distjobs.common.repository.JobRepository;
import com.distjobs.worker.process.JobProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
public class JobHandlingService {

    private static final Logger log = LoggerFactory.getLogger(JobHandlingService.class);

    private final JobRepository jobRepository;
    private final JobProcessor jobProcessor;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int maxFailuresBeforeDlq;
    private final long backoffBaseMs;
    private final long backoffMaxMs;
    private final Counter completed;
    private final Counter failedRetry;
    private final Counter dlq;

    public JobHandlingService(JobRepository jobRepository,
            JobProcessor jobProcessor,
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${worker.max-failures-before-dlq:3}") int maxFailuresBeforeDlq,
            @Value("${worker.backoff-base-ms:1000}") long backoffBaseMs,
            @Value("${worker.backoff-max-ms:300000}") long backoffMaxMs) {
        this.jobRepository = jobRepository;
        this.jobProcessor = jobProcessor;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.maxFailuresBeforeDlq = maxFailuresBeforeDlq;
        this.backoffBaseMs = backoffBaseMs;
        this.backoffMaxMs = backoffMaxMs;
        this.completed = meterRegistry.counter("worker.jobs.completed");
        this.failedRetry = meterRegistry.counter("worker.jobs.failed_retry_scheduled");
        this.dlq = meterRegistry.counter("worker.jobs.dlq");
    }

    @Transactional
    public void handle(JobMessage msg) throws Exception {
        int claimed = jobRepository.claimForProcessing(msg.jobId(), JobStatus.QUEUED, JobStatus.RUNNING);
        if (claimed == 0) {
            log.debug("Skip job {} — not in QUEUED (duplicate or already handled)", msg.jobId());
            return;
        }

        Job job = jobRepository.findById(msg.jobId()).orElseThrow();

        try {
            jobProcessor.process(job);
        } catch (Exception ex) {
            onFailure(job, msg, ex);
            return;
        }

        job.setStatus(JobStatus.COMPLETED);
        jobRepository.save(job);
        completed.increment();
        log.info("Job {} completed", job.getId());
    }

    private void onFailure(Job job, JobMessage msg, Exception ex) throws Exception {
        log.warn("Job {} failed: {}", job.getId(), ex.getMessage());
        int newFailures = job.getRetryCount() + 1;
        job.setRetryCount(newFailures);

        if (newFailures >= maxFailuresBeforeDlq) {
            job.setStatus(JobStatus.FAILED);
            jobRepository.save(job);
            String json = objectMapper.writeValueAsString(msg);
            kafkaTemplate.send(JobTopics.DLQ, job.getId().toString(), json).get();
            dlq.increment();
            log.warn("Job {} sent to DLQ after {} failures", job.getId(), newFailures);
            return;
        }

        long delayMs = Math.min(backoffMaxMs, backoffBaseMs * (1L << Math.min(30, newFailures - 1)));
        job.setStatus(JobStatus.PENDING);
        job.setScheduleTime(Instant.now().plus(delayMs, ChronoUnit.MILLIS));
        jobRepository.save(job);
        failedRetry.increment();
        log.info("Job {} rescheduled with backoff {} ms (failure {}/{})",
                job.getId(), delayMs, newFailures, maxFailuresBeforeDlq);
    }
}
