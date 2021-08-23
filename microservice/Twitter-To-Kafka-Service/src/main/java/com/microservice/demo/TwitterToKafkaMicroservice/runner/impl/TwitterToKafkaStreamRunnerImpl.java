package com.microservice.demo.TwitterToKafkaMicroservice.runner.impl;

import com.microservice.demo.TwitterToKafkaMicroservice.listener.TwitterKafkaStatusListener;
import com.microservice.demo.TwitterToKafkaMicroservice.runner.StreamRunner;
import com.microservice.demo.config.TwitterToKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.mock-tweet-enabled",havingValue = "false",matchIfMissing = true)
public class TwitterToKafkaStreamRunnerImpl implements StreamRunner {
    private final TwitterToKafkaConfig twitterToKafkaConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;
    private final Logger logger = LoggerFactory.getLogger(TwitterToKafkaStreamRunnerImpl.class);

    public TwitterToKafkaStreamRunnerImpl(TwitterToKafkaConfig twitterToKafkaConfig, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @PreDestroy
    void shutDown() {
        if (twitterStream != null) {
            logger.info("SHUT DOWN Runner");
            twitterStream.shutdown();
        }
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();

    }

    private void addFilter() {
        var keyWords = twitterToKafkaConfig.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keyWords);
        twitterStream.filter(filterQuery);
    }
}
