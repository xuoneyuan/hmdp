package com.hmdp.event;

import com.alibaba.fastjson.JSONObject;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Event;
import com.hmdp.service.IBlogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.ognl.IntHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.hmdp.utils.KafkaConstants.TOPIC_LIKE_BEHAVIOR;
import static com.hmdp.utils.KafkaConstants.TOPIC_SAVE_DB_FAILED;

@Component
@Slf4j
public class KafkaLikeConsumer {

    @Resource
    private IBlogService blogService;
    @Resource
    private KafkaLikeProducer kafkaLikeProducer;

    Map<String,Integer>  blogLikeCount = new HashMap<>();
    Map<String,Integer>  userLikeCount = new HashMap<>();
    Map<Blog,Acknowledgment> acknowledgment = new HashMap<>();

    @KafkaListener(topics = TOPIC_LIKE_BEHAVIOR)
    public void likeBehaviorMsgConsumer(ConsumerRecord record, Acknowledgment acknowledgment){

      if(record == null || record.value() == null){
          log.error("消息的内容为空");
          return;
      }
      Event event = JSONObject.parseObject(record.value().toString(),Event.class);
      if(event == null){
          log.error("消息格式错误");
          return;
      }

      Map<String,Object> data = event.getData();
      Long blogId = Long.valueOf(data.get("blogId").toString());
      Long userId = event.getUserId();
      Integer type = Integer.valueOf(data.get("type").toString());
      Long behaviorId = Long.valueOf(data.get("behaviorId").toString());

        LocalDateTime time = LocalDateTime.now();

        Blog blogLike = new Blog()
                .setBlogId(blogId)
                .setBehaviorId(behaviorId)
                .setUserId(userId)
                .setType(type)
                .setCreateTime(time);

        int tryCount = 0;
        try{
            while(!blogService.save(blogLike)){
                if(++tryCount<100){
                    Thread.sleep(100);
                }
            }
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            if(tryCount>=100){
                HashMap<String, Object> likeData = new HashMap<>();
                likeData.put("behaviorId",behaviorId);
                likeData.put("blogId",blogId);
                likeData.put("type",type);
                likeData.put("time",time);
                Event saveFailedEvent = new Event()
                        .setUserId(userId)
                        .setTopic(TOPIC_SAVE_DB_FAILED)
                        .setData(likeData);
                kafkaLikeProducer.publishEvent(saveFailedEvent);
            }
        }

        int diff = type == 1?1:-1;
        blogLikeCount.put(blogId.toString(),blogLikeCount.getOrDefault(blogId.toString(),0)+diff);
        userLikeCount.put(userId.toString(),userLikeCount.getOrDefault(userId.toString(),0)+diff);


    }

    @KafkaListener(topics = TOPIC_SAVE_DB_FAILED)
    public void saveFailedMsgConsumer(ConsumerRecord record,Acknowledgment ack){

        if(record == null || record.value() == null){
            log.error("消息的内容为空");
            return;
        }
        Event event = JSONObject.parseObject(record.value().toString(),Event.class);
        if(event == null){
            log.error("消息格式错误");
            return;
        }
        Map<String,Object> data = event.getData();
        Long blogId = Long.valueOf(data.get("blogId").toString());
        Long userId = event.getUserId();
        blogLikeCount.put(blogId.toString(),blogLikeCount.getOrDefault(blogId.toString(),0)-1);
        userLikeCount.put(userId.toString(),blogLikeCount.getOrDefault(userId.toString(),0)-1);



    }

    @Scheduled(fixedRate = 3000)
    private void flush(){
        if(!blogLikeCount.isEmpty()){
            ArrayList<Blog> countList = new ArrayList<>();

            for(Map.Entry<String,Integer>entry:blogLikeCount.entrySet()){
                Long blogId = Long.valueOf(entry.getKey());
                Integer count = entry.getValue();
                countList.add(new Blog().setBlogId(blogId).setLiked(count));
            }
            blogService.updateBatchCount(countList);
            acknowledgment.clear();
        }

    }





}
