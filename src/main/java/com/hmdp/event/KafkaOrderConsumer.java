package com.hmdp.event;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Event;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import static com.hmdp.utils.KafkaConstants.TOPIC_CREATE_ORDER;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class KafkaOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    //消费下单事件
    @KafkaListener(topics={TOPIC_CREATE_ORDER})
    public void handleCreateOrder(ConsumerRecord record){
        if(record == null || record.value()==null){
            log.error("消息为空");
            return;
        }
        Event event = JSONUtil.toBean(record.value().toString(),Event.class);
        if(event==null){
            log.error("信息格式错误");
            return;
        }

        //提交任务到线程池

        executorService.submit(() ->{
            Map<String, Object> data = event.getData();
            VoucherOrder voucherOrder = new VoucherOrder()
                    .setId(event.getEntityId())
                    .setUserId(event.getUserId())
                    .setBuyNumber(Integer.valueOf(data.get("buyNumber").toString()))
                    .setVoucherId(Long.valueOf(data.get("voucherId").toString()));

            voucherOrderService.createVoucherOrder(voucherOrder);

        });

    }

    @PreDestroy
    public void shutdown(){
        executorService.shutdown();
        try{
            if(!executorService.awaitTermination(60,TimeUnit.SECONDS)){
                executorService.shutdownNow();
            }
        }catch (InterruptedException e){
            executorService.shutdownNow();
        }
    }
}
