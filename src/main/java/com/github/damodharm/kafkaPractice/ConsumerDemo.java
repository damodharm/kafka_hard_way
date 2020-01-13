package com.github.damodharm.kafkaPractice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    @Bean
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "input-topic-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    // create consumer
    @Bean
    public KafkaConsumer<String, String> createConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return  consumer;
    }

    // subscribe to topic
    @PostConstruct
    public void subscribeToTopic() {
        KafkaConsumer<String, String> consumer = createConsumer();
        while(true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record: records) {
            logger.info(String.format("Topic : %s - Key : %s - Value : %s - Topic : %s - Partition : %s", record.topic(), record.key(), record.value(), record.topic(), record.partition()));
        }
        consumer.commitAsync();
        }
    }

    /*
    * As we are using consumer groups. If only one consumer is running then it will read from all the partitions.
    * If other consumer comes up then rebalancing happens then one consumer will read from one partition and the other consumer will read from other partition.
    * See the logs for clear understanding of rebalancing in kafkaConsumerGroups.
    * */
}
