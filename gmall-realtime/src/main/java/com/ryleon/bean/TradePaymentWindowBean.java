package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author ALiang
 * @date 2023-01-03
 * @effect DWS交易域支付事务时表-首次支付和当日独立用户支付Bean
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;
}

