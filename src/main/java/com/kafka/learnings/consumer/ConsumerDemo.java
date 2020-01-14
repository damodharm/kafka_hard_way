package com.kafka.learnings.consumer;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;

@Component
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerDemo {


    @Value("${topics.topic-name}")
    private String topicName;
    private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    @Autowired
    KafkaConsumer<String, String> consumer;

    @PostConstruct
    public void subscribeToTopicAndPollData() {
        consumer.subscribe(Collections.singletonList(topicName));

        while(true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record: records) {
            logger.info(String.format("Topic : %s - Key : %s - Value : %s - Partition : %s", record.topic(), record.key(), record.value(), record.partition()));
        }
        }
    }

    /*
    * As we are using consumer groups. If only one consumer is running then it will read from all the partitions.
    * If other consumer comes up then rebalancing happens then one consumer will read from one partition and the other consumer will read from other partition.
    * See the logs for clear understanding of rebalancing in kafkaConsumerGroups.
    * */
}
