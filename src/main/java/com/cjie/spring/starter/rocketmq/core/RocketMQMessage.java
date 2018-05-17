package com.cjie.spring.starter.rocketmq.core;



import com.alibaba.fastjson.JSON;
import lombok.Builder;
import lombok.Data;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * Created by 123 on 2016/5/31.
 */
@Data
@Builder
public final class RocketMQMessage<T extends Serializable> {
    private String topic;//最大长度 64
    private String tags;
    private T body;
    private String key;//最大长度 64
    private String messageId;
    private String consumeGroup;
    private int delayLevel;//延迟级别
    private MessageExt ext;


    protected Message covertMq() throws IOException {
        Message message = new Message();
        message.setBody(JSON.toJSONString(body).getBytes(Charset.forName("utf-8")));
        message.setTopic(topic);
        message.setTags(tags);
        message.setKeys(key);
        message.setDelayTimeLevel(delayLevel);
        return message;
    }
}
