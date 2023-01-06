package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author ALiang
 * @date 2023-01-04
 * @effect 交易域下单各窗口汇总表--统计当日下单独立用户数和新增下单用户数
 */

@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口关闭时间
    String edt;

    // 下单独立用户数
    Long orderUniqueUserCount;

    // 下单新用户数
    Long orderNewUserCount;

    // 下单活动减免金额
    BigDecimal orderActivityReduceAmount;

    // 下单优惠券减免金额
    BigDecimal orderCouponReduceAmount;

    // 下单原始金额
    BigDecimal orderOriginalTotalAmount;

    // 时间戳
    Long ts;

}
