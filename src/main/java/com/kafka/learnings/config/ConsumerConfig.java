package com.kafka.learnings.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class ConsumerConfig {

    @Value("${kafka.bootstrapUrl}")
    private String brokerUrl;
    @Value("${kafka.consumer.group-name}")
    private String consumerGroup;
    @Value("${kafka.consumer.offset-config}")
    private String offsetConfig;

    @Autowired
    @Qualifier("consumerProperties")
    Properties properties;

    @Bean(name = "consumerProperties")
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
        return properties;
    }

    @Bean
    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(properties);
    }
}
