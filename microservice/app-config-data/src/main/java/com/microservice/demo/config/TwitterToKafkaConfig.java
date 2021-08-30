package com.microservice.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaConfig {
    private List<String> twitterKeywords;
    private String welcomeMessage;
    private boolean mockTweetEnabled;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
    private Integer mockSleepMs;
}
