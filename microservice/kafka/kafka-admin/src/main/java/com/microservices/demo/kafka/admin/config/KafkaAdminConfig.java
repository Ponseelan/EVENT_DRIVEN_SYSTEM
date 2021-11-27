package com.microservices.demo.kafka.admin.config;

import com.microservice.demo.config.KafkaConfigData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import java.util.Map;

@EnableRetry
@Configuration
public class KafkaAdminConfig {
    private final KafkaConfigData kafkaConfigdata;

    public KafkaAdminConfig(KafkaConfigData kafkaConfigdata) {
        this.kafkaConfigdata = kafkaConfigdata;
    }
    @Bean
    public AdminClient adminClient()
    {
        return AdminClient.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,kafkaConfigdata.getBootStrapServer()));
    }
}
