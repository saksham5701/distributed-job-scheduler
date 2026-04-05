package com.distjobs.api.service;

import com.distjobs.api.dto.CreateJobRequest;
import com.distjobs.api.dto.JobResponse;
import com.distjobs.common.model.Job;
import com.distjobs.common.model.JobStatus;
import com.distjobs.common.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobRepository;

    public JobService(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Transactional
    public JobResponse create(CreateJobRequest request) {
        Job job = new Job();
        job.setStatus(JobStatus.PENDING);
        job.setScheduleTime(request.scheduleTime() != null ? request.scheduleTime() : Instant.now());
        job.setPayload(request.payload());
        job.setPriority(request.priority());
        job.setRetryCount(0);
        Job saved = jobRepository.save(job);
        return toResponse(saved);
    }

    @Transactional(readOnly = true)
    public JobResponse getById(UUID id) {
        return jobRepository.findById(id)
                .map(this::toResponse)
                .orElseThrow(() -> new JobNotFoundException(id));
    }

    private JobResponse toResponse(Job job) {
        return new JobResponse(
                job.getId(),
                job.getStatus(),
                job.getScheduleTime(),
                job.getRetryCount(),
                job.getPayload(),
                job.getPriority(),
                job.getCreatedAt(),
                job.getUpdatedAt());
    }
}
