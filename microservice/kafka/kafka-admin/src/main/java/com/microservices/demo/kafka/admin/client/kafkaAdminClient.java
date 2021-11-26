package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.common.config.KafkaConfigData;
import com.microservices.demo.common.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaAdminClientException;
import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class kafkaAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public kafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new KafkaAdminClientException("Reached max number of Attempts in Retry");
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicsToCreate = kafkaConfigData.getTopicNameToCreate();
        var topics = topicsToCreate.stream().map((topicName) -> {
            return new NewTopic(topicName.trim(), kafkaConfigData.getNumOfPartitions(), kafkaConfigData.getReplicationFactor());
        }).collect(Collectors.toList());
        return adminClient.createTopics(topics);
    }


    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable r) {
            throw new KafkaAdminClientException("Reached max Number of retry", r);
        }

        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOG.info("Inside the GetOPic listing");
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();
        if (topicListings != null) {
            topicListings.forEach((topic) -> LOG.debug("topic name is " + topic.name()));
        }
        return topicListings;
    }

    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic : kafkaConfigData.getTopicNameToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkmaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = 1;
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeInms = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkmaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeInms);
            sleepTimeInms *= multiplier;
        }
    }

    private void sleep(Long sleepTimeINMs) {
        try {
            Thread.sleep(sleepTimeINMs);
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }
    }

    private void checkmaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry)
            throw new KafkaAdminClientException("Max retry Attempt is made for create TOpics");

    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null)
            return false;
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }
}

