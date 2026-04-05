package com.distjobs.common.repository;

import com.distjobs.common.model.Job;
import com.distjobs.common.model.JobStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    /**
     * Jobs ready to enqueue: PENDING and due. Ordered by priority then schedule time.
     * Limited batch to bound DB and Kafka pressure per tick.
     */
    @Query("""
            SELECT j FROM Job j
            WHERE j.status = :status AND j.scheduleTime <= :now
            ORDER BY j.priority DESC, j.scheduleTime ASC
            """)
    List<Job> findDuePending(@Param("status") JobStatus status, @Param("now") Instant now,
            Pageable pageable);

    /**
     * Atomic claim for worker idempotency: only one consumer wins QUEUED → RUNNING.
     * Returns rows updated (1 if claimed, 0 if another worker or scheduler already moved it).
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Job j SET j.status = :newStatus WHERE j.id = :id AND j.status = :expectedStatus")
    int claimForProcessing(@Param("id") UUID id,
            @Param("expectedStatus") JobStatus expectedStatus,
            @Param("newStatus") JobStatus newStatus);
}
