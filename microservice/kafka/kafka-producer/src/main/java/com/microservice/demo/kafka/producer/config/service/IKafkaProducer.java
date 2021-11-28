package com.microservice.demo.kafka.producer.config.service;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.Serializable;

public interface IKafkaProducer<K extends Serializable,V extends SpecificRecordBase> {
    void send(String topicName,K key,V message);
}
