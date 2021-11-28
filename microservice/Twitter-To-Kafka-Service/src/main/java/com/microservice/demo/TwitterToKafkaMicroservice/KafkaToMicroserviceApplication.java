package com.microservice.demo.TwitterToKafkaMicroservice;

import com.microservice.demo.TwitterToKafkaMicroservice.init.IStreamInitializer;
import com.microservice.demo.TwitterToKafkaMicroservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages = "com.microservice.demo")
public class KafkaToMicroserviceApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToMicroserviceApplication.class);
    private final StreamRunner streamRunner;
    private IStreamInitializer streamInitializer;

    public KafkaToMicroserviceApplication(StreamRunner streamRunner, IStreamInitializer streamInitializer) {
        this.streamInitializer = streamInitializer;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaToMicroserviceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("INIT Action");
        streamInitializer.init();
        streamRunner.start();
    }
}
