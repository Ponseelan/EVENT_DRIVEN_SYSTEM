package com.microservice.demo.TwitterToKafkaMicroservice.listener;

import com.microservice.demo.TwitterToKafkaMicroservice.KafkaToMicroserviceApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToMicroserviceApplication.class);

    @Override
    public void onStatus(Status status) {
        logger.info("Twitter Status changed to" + status.getText());
    }
}
