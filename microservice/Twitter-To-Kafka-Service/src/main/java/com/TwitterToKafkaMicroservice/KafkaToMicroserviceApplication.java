package com.TwitterToKafkaMicroservice;

import com.TwitterToKafkaMicroservice.config.TwitterToKafkaConfig;
import com.TwitterToKafkaMicroservice.runner.StreamRunner;
import com.TwitterToKafkaMicroservice.runner.impl.TwitterToKafkaStreamRunnerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class KafkaToMicroserviceApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToMicroserviceApplication.class);
    private final StreamRunner streamRunner;
    private final TwitterToKafkaConfig twitterToKafkaConfig;

    public KafkaToMicroserviceApplication(TwitterToKafkaConfig twitterToKafkaConfig, StreamRunner streamRunner) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaToMicroserviceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("INIT Action");
        twitterToKafkaConfig.getTwitterKeywords().stream().forEach((x) -> {
            logger.info(x);
        });
        logger.info(twitterToKafkaConfig.getWelcomeMessage());
        streamRunner.start();
    }
}
