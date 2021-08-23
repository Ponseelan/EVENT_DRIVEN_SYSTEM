package com.microservice.demo.TwitterToKafkaMicroservice.runner.impl;

import com.microservice.demo.TwitterToKafkaMicroservice.Exceptions.TwitterToKafkaException;
import com.microservice.demo.TwitterToKafkaMicroservice.listener.TwitterKafkaStatusListener;
import com.microservice.demo.TwitterToKafkaMicroservice.runner.StreamRunner;
import com.microservice.demo.config.TwitterToKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.mock-tweet-enabled", havingValue = "true")
public class TwitterToKafkaStreamRunnerMock implements StreamRunner {
    private static final String[] WORDS = new String[]{
            "LOREM",
            "IPSUM", "ODOR", "PONSEELAN", "JEBASING"
    };
    private static final String tweetasRawJson = "{" + "\"createdAt\":\"{0}\","
            + "\"id\":\"{1}\","
            + "" + "\"text\":\"{2}\","
            + "\"user\":{\"id\":\"{3}\"}}";
    private final TwitterToKafkaConfig twitterToKafkaConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    private final Random random = new Random();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public TwitterToKafkaStreamRunnerMock(TwitterToKafkaConfig twitterToKafkaConfig, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        logger.info("status.getText()");
        String[] keywords = twitterToKafkaConfig.getTwitterKeywords().toArray(new String[0]);
        long minTweetLength = twitterToKafkaConfig.getMockMinTweetLength();
        long maxTweetLength = twitterToKafkaConfig.getMockMaxTweetLength();
        long mockSleepMs = twitterToKafkaConfig.getMockSleepMs();
        sendTweets(keywords, minTweetLength, maxTweetLength, mockSleepMs);
    }

    private void sendTweets(String[] keywords, long minTweetLength, long maxTweetLength, long mockSleepMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
                    while (true) {
                        String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                        Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                        twitterKafkaStatusListener.onStatus(status);
                        sleepThread(mockSleepMs);
                    }
                }
        );
    }

    private void sleepThread(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
            throw new TwitterToKafkaException(ex.getMessage());
        }
    }

    private String getFormattedTweet(String[] keywords, long mintweetLengtth, long gmaxTweetLength) {
        String[] params = new String[]{ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, mintweetLengtth, gmaxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))};
        String tweet = tweetasRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, long mintweetLengtth, long gmaxTweetLength) {
        String value = "PONSEELAN";
        int tweetLength = random.nextInt();
        return value + tweetLength;
    }
}
