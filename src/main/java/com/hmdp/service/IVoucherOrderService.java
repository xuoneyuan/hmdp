package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import org.jetbrains.annotations.NotNull;
import org.springframework.transaction.annotation.Transactional;

/**
 * ivoucher订单服务
 * <p>
 * 服务类
 * </p>
 *
 * @author 虎哥
 * @date 2022/10/09
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {


    Result seckillVoucher(Long voucherId,int buyNumber);

    void createVoucherOrder(VoucherOrder voucherOrder);

    Result payment(OrderPaymentDTO orderPaymentDTO);

    Result commonVoucher(Long voucherId, int buyNumber);

    Result limitVoucher(Long voucherId, int buyNumber);

}