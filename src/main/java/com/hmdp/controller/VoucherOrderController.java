package com.hmdp.controller;


import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("/seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        int buyNumber = 1;
        return voucherOrderService.secKillVoucher(voucherId,buyNumber);
    }

    @PostMapping("/common/{id}")
    public Result commonVoucher (@PathVariable("id") Long voucherId){
        int buyNumber = 1;
        return voucherOrderService.commonVoucher(voucherId, buyNumber);
    }

    @PostMapping("/payment")
    public Result payment(@RequestBody OrderPaymentDTO orderPaymentDTO){
        return voucherOrderService.payment(orderPaymentDTO);
    }
}
