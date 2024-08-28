package com.hmdp.event;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Event;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class KafkaLikeProducer {

    @Resource
    KafkaTemplate kafkaTemplate;

    public void publishEvent(Event event){
        kafkaTemplate.send(event.getTopic(), JSONUtil.toJsonStr(event));
    }
}
