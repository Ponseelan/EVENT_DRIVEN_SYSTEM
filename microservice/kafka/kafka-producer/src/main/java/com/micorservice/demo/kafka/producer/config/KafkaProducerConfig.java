package com.micorservice.demo.kafka.producer.config;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.config.KafkaProducerConfigData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig <K extends Serializable,V extends SpecificRecordBase>{
    private KafkaConfigData kafkaConfigData;
    private final KafkaProducerConfigData kafkaProducerConfig;

    public KafkaProducerConfig(KafkaConfigData kafkaConfigData, KafkaProducerConfigData kafkaProducerConfigData) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducerConfig = kafkaProducerConfigData;
    }
    @Bean
    public Map<String,Object> producerConfig()
    {
        Map<String,Object> props=new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaConfigData.getBootStrapServer());
        props.put(kafkaConfigData.getSchemaRegistryUrlKey(),kafkaConfigData.getSchemaRegistryUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaProducerConfig.getKeySerializerClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,kafkaProducerConfig.getValueSerializerClass());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaProducerConfig.getBatchSize()* kafkaProducerConfig.getBatchSizeBoostFactor());
        props.put(ProducerConfig.LINGER_MS_CONFIG,kafkaProducerConfig.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,kafkaProducerConfig.getCompressionType());
        props.put(ProducerConfig.ACKS_CONFIG,kafkaProducerConfig.getAcks());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,kafkaProducerConfig.getRequestTimeOutMs());
        props.put(ProducerConfig.RETRIES_CONFIG,kafkaProducerConfig.getRetryCount());
        return props;
    }
    @Bean
    public ProducerFactory<K,V> producerfactory()
    {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<K,V> kafkaTemplate()
    {
        return new KafkaTemplate<>(producerfactory());
    }

}
