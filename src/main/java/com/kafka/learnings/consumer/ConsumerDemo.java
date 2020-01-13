package com.kafka.learnings.consumer;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
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
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerDemo {

    @Value("${kafka.consumer.group-name}")
    private String consumerGroup;
    @Value("${kafka.bootstrapUrl}")
    private String brokerUrl;
    @Value("${kafka.topic-name}")
    private String topicName;
    private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    // create consumer config
    @Bean
    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    // create consumer
    @Bean
    private KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(properties());
    }

    // subscribe consumer to topic(s)
    @PostConstruct
    public void subscribeToTopic() {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(topicName));

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
