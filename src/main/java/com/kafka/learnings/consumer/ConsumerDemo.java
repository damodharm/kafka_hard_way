package com.kafka.learnings.consumer;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
public class ConsumerDemo {

    @Value("${topics.topic-name}")
    private String topicName;
    private final Tracer tracer;
    private final TracingKafkaConsumer<String, String> tracingKafkaConsumer;
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public ConsumerDemo(TracingKafkaConsumer<String, String> tracingKafkaConsumer) {
        this.tracingKafkaConsumer = tracingKafkaConsumer;
        tracer = GlobalTracer.get();
    }

    @EventListener(ContextRefreshedEvent.class)
    public void contextRefreshedEvent() {
        listen();
    }

    private void listen() {
        tracingKafkaConsumer.subscribe(Collections.singletonList(topicName));
        boolean forever = true;
        while (forever) {

            ConsumerRecords<String, String> records = tracingKafkaConsumer.poll(100L);

            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("Topic : %s - Key : %s - Value : %s - Partition : %s", record.topic(), record.key(), record.value(), record.partition()));
            }
        }
    }
}
