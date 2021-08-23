package com.microservice.demo.TwitterToKafkaMicroservice.Exceptions;

public class TwitterToKafkaException extends  RuntimeException{
    public  TwitterToKafkaException()
    {
        super();
    }
    public  TwitterToKafkaException(String message)
    {
        super(message);
    }
    public  TwitterToKafkaException(String message,Throwable throwable)
    {
        super(message,throwable);
    }

}
