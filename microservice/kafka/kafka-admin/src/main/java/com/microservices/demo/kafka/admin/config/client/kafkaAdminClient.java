package com.microservices.demo.kafka.admin.config.client;

import com.microservices.demo.common.config.KafkaConfigData;
import com.microservices.demo.common.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.RetryException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;
import java.util.stream.Collectors;


public class kafkaAdminClient {
    private static final Logger LOG= LoggerFactory.getLogger(KafkaAdminClient.class);
private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
private final RetryTemplate retryTemplate;
    public kafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient,RetryTemplate retryTemplate) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate=retryTemplate;
    }
    public void createTopics()
    {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult=retryTemplate.execute(this::doCreateTopics);
        }
        catch(Throwable t){
            throw new RetryException("Reached max number of Attempts in Retry");
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicsToCreate= kafkaConfigData.getTopicNameToCreate();
        var topics=topicsToCreate.stream().map((topicName)->{return new NewTopic(topicName.trim(),kafkaConfigData.getNumOfPartitions(), kafkaConfigData.getReplicationFactor());}).collect(Collectors.toList());
        return adminClient.createTopics(topics);
    };

    public void checkTopicsCreated(){}
}
