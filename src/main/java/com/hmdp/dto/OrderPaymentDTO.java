package com.hmdp.dto;

import lombok.Data;

@Data
public class OrderPaymentDTO {
    //订单号
    private Long orderId;

    //支付方式
    private Integer payType;
}
