package com.microservice.demo.TwitterToKafkaMicroservice.transformer;

import com.microservice.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class TwitterStatusToAvuraTransformer {
    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status)
    {
        return  TwitterAvroModel.newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt().toLocaleString())
                .build();
    }
}
