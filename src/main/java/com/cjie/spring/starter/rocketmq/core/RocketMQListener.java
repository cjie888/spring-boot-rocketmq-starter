

package com.cjie.spring.starter.rocketmq.core;

public interface RocketMQListener<T> {
    void onMessage(T message);
}
