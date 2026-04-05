package com.distjobs.scheduler.lock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Single-key Redis lock (SETNX + TTL) so only one scheduler instance polls/enqueues per tick.
 * Value is a random token; TTL ensures recovery if the holder crashes before unlock.
 */
@Component
public class SchedulerDistributedLock {

    private static final String LOCK_KEY = "scheduler:dispatch:lock";

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public SchedulerDistributedLock(StringRedisTemplate redis,
            @Value("${scheduler.lock-ttl-seconds:30}") long lockTtlSeconds) {
        this.redis = redis;
        this.ttl = Duration.ofSeconds(lockTtlSeconds);
    }

    /**
     * @return lock token if acquired; empty if another instance holds the lock
     */
    public Optional<String> tryLock() {
        String token = UUID.randomUUID().toString();
        Boolean ok = redis.opsForValue().setIfAbsent(LOCK_KEY, token, ttl);
        if (Boolean.TRUE.equals(ok)) {
            return Optional.of(token);
        }
        return Optional.empty();
    }

    public void unlock(String token) {
        String current = redis.opsForValue().get(LOCK_KEY);
        if (token != null && token.equals(current)) {
            redis.delete(LOCK_KEY);
        }
    }
}
