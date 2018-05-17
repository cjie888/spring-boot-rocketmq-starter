package com.cjie.spring.starter.rocketmq.core;

public class MessagingException extends RuntimeException {

    public MessagingException(String message, Exception e) {
        super(message, e);
    }

    public MessagingException(String message) {
        super(message);
    }

}
