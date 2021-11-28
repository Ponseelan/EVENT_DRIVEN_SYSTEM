package com.microservice.demo.TwitterToKafkaMicroservice.listener;

import com.microservice.demo.kafka.producer.config.service.IKafkaProducer;
import com.microservice.demo.TwitterToKafkaMicroservice.KafkaToMicroserviceApplication;
import com.microservice.demo.TwitterToKafkaMicroservice.transformer.TwitterStatusToAvuraTransformer;
import com.microservice.demo.config.KafkaConfigData;
import com.microservice.kafka.avro.model.TwitterAvroModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToMicroserviceApplication.class);
    private final KafkaConfigData kafkaConfigData;
    private final IKafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvuraTransformer twitterStatusToAvuraTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData, TwitterStatusToAvuraTransformer twitterStatusToAvuraTransformer,IKafkaProducer kafkaProducer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvuraTransformer = twitterStatusToAvuraTransformer;
    }

    @Override
    public void onStatus(Status status) {
        logger.info("Twitter Status changed to" + status.getText());
        TwitterAvroModel twitterAvroModel=twitterStatusToAvuraTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(),twitterAvroModel.getUserId(),twitterAvroModel);
    }
}
