package com.microservice.demo.TwitterToKafkaMicroservice.init.impl;

import com.microservice.demo.TwitterToKafkaMicroservice.init.IStreamInitializer;
import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.kafka.admin.client.kafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements IStreamInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamInitializer.class);
    private final KafkaConfigData kafkaConfigData;
    private final kafkaAdminClient kafkaadminClient;

    public KafkaStreamInitializer(KafkaConfigData kafkaConfigData, kafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaadminClient = kafkaAdminClient;
    }

    @Override
    public void init() {
        kafkaadminClient.createTopics();
        kafkaadminClient.checkSchemaRegistry();
        LOG.info("Topics with name {} is ready for operations", kafkaConfigData.getTopicNameToCreate().toArray());
    }
}
