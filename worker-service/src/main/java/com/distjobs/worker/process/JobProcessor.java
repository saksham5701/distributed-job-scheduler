package com.distjobs.worker.process;

import com.distjobs.common.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Demo processing: succeeds unless payload contains the substring "FAIL" (case-sensitive).
 * Replace with real side-effecting work; keep idempotent where the domain allows it.
 */
@Component
public class JobProcessor {

    private static final Logger log = LoggerFactory.getLogger(JobProcessor.class);

    public void process(Job job) throws Exception {
        log.info("Processing job {} (retryCount={})", job.getId(), job.getRetryCount());
        if (job.getPayload() != null && job.getPayload().contains("FAIL")) {
            throw new IllegalStateException("Simulated failure for payload containing FAIL");
        }
        Thread.sleep(50);
    }
}
