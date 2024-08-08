package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.*;
import com.hmdp.event.KafkaOrderProducer;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.quartz.LocalDataSourceJobStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.hmdp.service.ICommonVoucherService;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.hmdp.utils.KafkaConstants.TOPIC_CREATE_ORDER;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.SystemConstants.MAX_BUY_LIMIT;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisTemplate redisTemplate;
    @Resource
    private KafkaOrderProducer kafkaOrderProducer;
    @Resource
    private ICommonVoucherService commonVoucherService;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    public void createVoucherOrder (VoucherOrder voucherOrder){
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        //创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order"+userId);
        //尝试获取锁
        boolean isLock = redisLock.tryLock();
        //没有获取锁
        if(!isLock){
            log.error("获取锁失败");
            return;
        }

        try{
            //查询订单
            Long count = lambdaQuery()
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .eq(VoucherOrder::getUserId, userId)
                    .count();
            //判断是否存在
            if(count>MAX_BUY_LIMIT){
                log.error("超过最大购买限制！");
                return;
            }
            //扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - " + voucherOrder.getBuyNumber()) // set stock = stock - buynumber
                    .eq("voucher_id", voucherId)
                    .gt("stock", voucherOrder.getBuyNumber()) // where voucher_id = ? and stock > buynumber
                    .update();
            if(!success){
                log.error("库存不足");
                return;
            }
            //创建订单
            save(voucherOrder);
        }finally {
            redisLock.unlock();
        }

    }
    @Override
    public Result secKillVoucher(Long voucherId, int buyNumber) {
        Long userId = UserHolder.getUser().getId();
        long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long orderId = redisIdWorker.nextId("order");
        try {
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    String.valueOf(orderId),
                    String.valueOf(currentTime),
                    String.valueOf(buyNumber),
                    String.valueOf(MAX_BUY_LIMIT)
            );
            switch (result.intValue()) {
                case 0:
                    sendOrderMsgToKafka(orderId, voucherId, userId, buyNumber);
                    return Result.ok(orderId);
                case 1:
                    return Result.fail("redis缺少数据");
                case 2:
                    return Result.fail("秒杀尚未开始");
                case 3:
                    return Result.fail("秒杀已经结束");
                case 4:
                    return Result.fail("库存不足");
                case 5:
                    return Result.fail("超过最大购买限制");
                default:
                    return Result.fail("未知错误");
            }

        } catch (Exception e) {
            log.error("处理订单异常");
            return Result.fail("未知错误");
        }
    }

    public void sendOrderMsgToKafka(long orderId,Long voucherId,Long userId,int buyNumber){
        Map<String,Object> data = new HashMap<>();
        data.put("voucherId",voucherId);
        data.put("buyNumber",buyNumber);
        Event event = new Event()
                .setTopic(TOPIC_CREATE_ORDER)
                .setUserId(userId)
                .setEntityId(orderId)
                .setData(data);

        kafkaOrderProducer.publishEvent(event);
    }




    @Override
    public Result payment(OrderPaymentDTO orderPaymentDTO){
        Long orderId = orderPaymentDTO.getOrderId();
        Integer payType = orderPaymentDTO.getPayType();
        //查询redis中是否有此订单
        boolean isRedisExist = redisTemplate.opsForSet().isMember(SECKILL_ORDER_KEY,orderId);
        //查询mysql中是否有此订单
        Long userId = UserHolder.getUser().getId();
        VoucherOrder voucherOrder = this.getOne(new LambdaQueryWrapper<VoucherOrder>()
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getId, orderId));
        if(isRedisExist){
            if(voucherOrder==null){
                try{
                    Thread.sleep(1000);
                    return payment(orderPaymentDTO);
                }catch (Exception e) {
                    return Result.fail("未知错误");
                  }
                }
            }else {
                if(voucherOrder==null){
                    return Result.fail("订单不存在");
                }
            }
            return Result.ok();
        }


    @Override
    public Result commonVoucher(Long voucherId, int buyNumber){
        //查询优惠券
        CommonVoucher voucher = commonVoucherService.getById(voucherId);

        //判断库存是否充足
        if(voucher.getStock()<buyNumber){
            return Result.fail("库存不足");
        }

        //乐观锁扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -"+buyNumber)
                .eq("voucherId",voucherId)
                .ge("stock",buyNumber)
                .update();

        if(!success){
            return Result.fail("库存不足");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        Long userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(voucherId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);

    }



}