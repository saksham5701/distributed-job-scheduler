package com.distjobs.worker.kafka;

import com.distjobs.common.kafka.JobMessage;
import com.distjobs.common.kafka.JobTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class JobsListener {

    private static final Logger log = LoggerFactory.getLogger(JobsListener.class);

    private final ObjectMapper objectMapper;
    private final JobHandlingService jobHandlingService;

    public JobsListener(ObjectMapper objectMapper, JobHandlingService jobHandlingService) {
        this.objectMapper = objectMapper;
        this.jobHandlingService = jobHandlingService;
    }

    @KafkaListener(topics = JobTopics.JOBS, groupId = "${spring.kafka.consumer.group-id:worker-service}")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        try {
            JobMessage msg = objectMapper.readValue(record.value(), JobMessage.class);
            jobHandlingService.handle(msg);
        } catch (Exception e) {
            log.error("Handle failed for record at {}-{}: {}", record.partition(), record.offset(), e.getMessage(), e);
            throw e;
        }
    }
}
