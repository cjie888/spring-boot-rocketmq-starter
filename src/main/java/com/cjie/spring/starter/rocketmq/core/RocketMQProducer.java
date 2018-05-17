
package com.cjie.spring.starter.rocketmq.core;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.Objects;

@Data
@Slf4j
public class RocketMQProducer implements InitializingBean, DisposableBean {

    private DefaultMQProducer producer;

    private String charset = "UTF-8";

    private MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    /**
     * <p> Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * Reliable synchronous transmission is used in extensive scenes, such as important notification messages, SMS
     * notification, SMS marketing system, etc.. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link DefaultMQProducer#getRetryTimesWhenSendFailed} times before claiming failure. As a result, multiple
     * messages may potentially delivered to broker(s). It's up to the application developers to resolve potential
     * duplication issue.
     *
     * @param message {@link RocketMQMessage}
     * @return {@link SendResult}
     */
    public SendResult syncSend(RocketMQMessage message) {
        return syncSend(message, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSend(RocketMQMessage)} with send timeout specified in addition.
     *
     * @param message {@link RocketMQMessage}
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSend(RocketMQMessage message, long timeout) throws MessagingException {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic()) || Objects.isNull(message.getBody())) {
            log.info("syncSend failed. destination:{}, message is null ", JSON.toJSONString(message));
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            SendResult sendResult = producer.send(rocketMsg, timeout);
            long costTime = System.currentTimeMillis() - now;
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            log.info("syncSend failed, message:{} ", message);
            throw new MessagingException(e.getMessage(), e);
        }
    }


    /**
     * Same to {@link #syncSend(RocketMQMessage)} with send orderly with hashKey by specified.
     *
     * @param message {@link RocketMQMessage}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(RocketMQMessage message, String hashKey) throws MessagingException {
        return syncSendOrderly(message, hashKey, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSendOrderly(RocketMQMessage, String)} with send timeout specified in addition.
     *
     * @param message {@link RocketMQMessage}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(RocketMQMessage message, String hashKey, long timeout) throws MessagingException {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic())) {
            log.info("syncSendOrderly failed. destination:{}, message is null ", JSON.toJSONString(message));
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            SendResult sendResult = producer.send(rocketMsg, messageQueueSelector, hashKey, timeout);
            long costTime = System.currentTimeMillis() - now;
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            log.info("syncSendOrderly failed. message:{} ", message);
            throw new MessagingException(e.getMessage(), e);
        }
    }



    /**
     * Same to {@link #asyncSend(RocketMQMessage, SendCallback)} with send timeout specified in addition.
     *
     * @param message {@link RocketMQMessage}
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSend(RocketMQMessage message, SendCallback sendCallback, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic()) || Objects.isNull(message.getBody())) {
            log.info("asyncSend failed. destination:{}, message is null ", JSON.toJSONString(message));
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            producer.send(rocketMsg, sendCallback, timeout);
        } catch (Exception e) {
            log.info("asyncSend failed. message:{} ", message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * <p> Send message to broker asynchronously. asynchronous transmission is generally used in response time sensitive
     * business scenarios. </p>
     *
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
     *
     * Similar to {@link #syncSend(RocketMQMessage)}, internal implementation would potentially retry up to {@link
     * DefaultMQProducer#getRetryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield
     * message duplication and application developers are the one to resolve this potential issue.
     *
     * @param message {@link RocketMQMessage}
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(RocketMQMessage message, SendCallback sendCallback) {
        asyncSend(message, sendCallback, producer.getSendMsgTimeout());
    }




    /**
     * Same to {@link #asyncSendOrderly(RocketMQMessage, String, SendCallback)} with send timeout specified in
     * addition.
     *
     * @param message {@link RocketMQMessage}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSendOrderly(RocketMQMessage message, String hashKey, SendCallback sendCallback,
        long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic()) || Objects.isNull(message.getBody())) {
            log.info("asyncSendOrderly failed. destination:{}, message is null ", JSON.toJSONString(message));
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            producer.send(rocketMsg, messageQueueSelector, hashKey, sendCallback, timeout);
        } catch (Exception e) {
            log.info("asyncSendOrderly failed, message:{} ", message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #asyncSend(RocketMQMessage, SendCallback)} with send orderly with hashKey by specified.
     *
     * @param message {@link RocketMQMessage}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendOrderly(RocketMQMessage message, String hashKey, SendCallback sendCallback) {
        asyncSendOrderly(message, hashKey, sendCallback, producer.getSendMsgTimeout());
    }





    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * One-way transmission is used for cases requiring moderate reliability, such as log collection.
     *
     * @param message {@link RocketMQMessage}
     */
    public void sendOneWay(RocketMQMessage message) {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic())  || Objects.isNull(message.getBody())) {
            log.info("sendOneWay failed. destination:{}, message is null ", JSON.toJSONString(message));
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            producer.sendOneway(rocketMsg);
        } catch (Exception e) {
            log.info("sendOneWay failed. message:{} ", message);
            throw new MessagingException(e.getMessage(), e);
        }
    }



    /**
     * Same to {@link #sendOneWay(RocketMQMessage)} with send orderly with hashKey by specified.
     *
     * @param message {@link RocketMQMessage}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     */
    public void sendOneWayOrderly(String destination, RocketMQMessage message, String hashKey) {
        if (Objects.isNull(message) || Objects.isNull(message.getTopic()) || Objects.isNull(message.getBody())) {
            log.info("sendOneWayOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = message.covertMq();
            producer.sendOneway(rocketMsg, messageQueueSelector, hashKey);
        } catch (Exception e) {
            log.info("sendOneWayOrderly failed. destination:{}, message:{}", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }



    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(producer, "Property 'producer' is required");
        producer.start();
    }


    @Override
    public void destroy() {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
    }
}
