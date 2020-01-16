package com.kafka.learnings.config;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class ConsumerConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    @Value("${kafka.consumer.offset-config}")
    private String offsetConfig;

    private final Tracer tracer;

    @Autowired
    @Qualifier("consumerProperties")
    private Properties properties;

    @Autowired
    private KafkaConsumer<String, String> consumer;

    public ConsumerConfiguration(Tracer tracer) {
        this.tracer = tracer;
    }

    @Bean(name = "consumerProperties")
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
        return properties;
    }

    @Bean
    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(properties);
    }

    @Bean
    public TracingKafkaConsumer<String, String> tracingKafkaConsumer() {
        return new TracingKafkaConsumer<>(consumer, tracer);
    }
}
