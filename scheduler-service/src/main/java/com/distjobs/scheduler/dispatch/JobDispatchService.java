package com.distjobs.scheduler.dispatch;

import com.distjobs.common.kafka.JobMessage;
import com.distjobs.common.kafka.JobTopics;
import com.distjobs.common.model.Job;
import com.distjobs.common.model.JobStatus;
import com.distjobs.common.repository.JobRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Per-job transaction: claim PENDING → QUEUED, then publish to Kafka with jobId as key
 * (stable partition ordering). If publish fails, the transaction rolls back so the job
 * stays PENDING and a later tick retries.
 */
@Service
public class JobDispatchService {

    private static final Logger log = LoggerFactory.getLogger(JobDispatchService.class);

    private final JobRepository jobRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final long kafkaSendTimeoutMs;
    private final Counter dispatched;
    private final Counter publishFailed;

    public JobDispatchService(JobRepository jobRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            TransactionTemplate transactionTemplate,
            MeterRegistry meterRegistry,
            @Value("${scheduler.batch-size:50}") int batchSize,
            @Value("${scheduler.kafka-send-timeout-ms:5000}") long kafkaSendTimeoutMs) {
        this.jobRepository = jobRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
        this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
        this.dispatched = meterRegistry.counter("scheduler.jobs.dispatched");
        this.publishFailed = meterRegistry.counter("scheduler.kafka.publish_failed");
    }

    public void dispatchDueJobs() {
        Instant now = Instant.now();
        List<Job> due = jobRepository.findDuePending(JobStatus.PENDING, now, PageRequest.of(0, batchSize));
        for (Job job : due) {
            try {
                dispatchOne(job.getId());
            } catch (Exception ex) {
                log.warn("Dispatch failed for job {}: {}", job.getId(), ex.getMessage());
            }
        }
    }

    private void dispatchOne(java.util.UUID jobId) {
        transactionTemplate.executeWithoutResult(status -> {
            int updated = jobRepository.claimForProcessing(jobId, JobStatus.PENDING, JobStatus.QUEUED);
            if (updated == 0) {
                return;
            }
            Job job = jobRepository.findById(jobId).orElseThrow();
            JobMessage msg = new JobMessage(job.getId(), job.getPayload(), job.getRetryCount(), job.getPriority());
            String json;
            try {
                json = objectMapper.writeValueAsString(msg);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(JobTopics.JOBS, job.getId().toString(), json);
            try {
                future.get(kafkaSendTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (ExecutionException | TimeoutException e) {
                publishFailed.increment();
                throw new IllegalStateException("Kafka publish failed for job " + jobId, e);
            }
            dispatched.increment();
            log.debug("Published job {} to {}", jobId, JobTopics.JOBS);
        });
    }
}
