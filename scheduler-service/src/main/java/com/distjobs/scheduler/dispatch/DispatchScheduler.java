package com.distjobs.scheduler.dispatch;

import com.distjobs.scheduler.lock.SchedulerDistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class DispatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(DispatchScheduler.class);

    private final SchedulerDistributedLock lock;
    private final JobDispatchService jobDispatchService;

    public DispatchScheduler(SchedulerDistributedLock lock, JobDispatchService jobDispatchService) {
        this.lock = lock;
        this.jobDispatchService = jobDispatchService;
    }

    @Scheduled(fixedDelayString = "${scheduler.tick-ms:3000}")
    public void tick() {
        Optional<String> token = lock.tryLock();
        if (token.isEmpty()) {
            log.trace("Scheduler lock not acquired, skipping tick");
            return;
        }
        try {
            jobDispatchService.dispatchDueJobs();
        } finally {
            lock.unlock(token.get());
        }
    }
}
