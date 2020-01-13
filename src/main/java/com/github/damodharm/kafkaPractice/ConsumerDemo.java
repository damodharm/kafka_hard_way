package com.github.damodharm.kafkaPractice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.scope.DefaultScopedObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class ConsumerDemo {

    @Value("${kafka.consumer.group-name}")
    private String CONSUMER_GROUP_NAME;
    @Value("${kafka.bootstrapUrl}")
    private String BROKER_URL;
    @Value("${kafka.topic-name}")
    private String TOPIC_NAME;
    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    // create properties
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "input-topic-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    // create consumer
    @Bean
    public KafkaConsumer<String, String> createConsumer() {
        System.out.println("In createConsumer() method....................");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return  consumer;
    }

    // subscribe to topic
    @PostConstruct
    public void subscribeToTopic() {
        System.out.println("Subscribed to topic.............................");
        ConsumerRecords<String, String> records = createConsumer().poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record: records) {
            logger.info(String.format("This data is being consumed by consumers. \nTopic : %s\nOffset : %d\nPartition : %d", record.topic(), record.offset(), record.partition()));
        }
    }
}
