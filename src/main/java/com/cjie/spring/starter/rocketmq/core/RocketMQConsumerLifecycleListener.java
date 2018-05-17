

package com.cjie.spring.starter.rocketmq.core;

public interface RocketMQConsumerLifecycleListener<T> {
    void prepareStart(final T consumer);
}
