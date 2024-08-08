package com.hmdp.event;

import cn.hutool.json.JSONUtil;
import org.springframework.stereotype.Component;
import org.springframework.kafka.core.KafkaTemplate;
import com.hmdp.entity.Event;
import javax.annotation.Resource;

@Component
public class KafkaOrderProducer {

    @Resource
    private KafkaTemplate kafkaTemplate;

    public void publishEvent(Event event){
        kafkaTemplate.send(event.getTopic(), JSONUtil.toJsonStr(event));

    }
}
