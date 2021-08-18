package com.TwitterToKafkaMicroservice.runner.impl;

import com.TwitterToKafkaMicroservice.config.TwitterToKafkaConfig;
import com.TwitterToKafkaMicroservice.listener.TwitterKafkaStatusListener;
import com.TwitterToKafkaMicroservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;

@Component
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
