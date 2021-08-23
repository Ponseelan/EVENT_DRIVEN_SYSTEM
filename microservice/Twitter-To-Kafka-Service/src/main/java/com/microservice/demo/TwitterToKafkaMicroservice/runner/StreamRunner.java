package com.microservice.demo.TwitterToKafkaMicroservice.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;
}
