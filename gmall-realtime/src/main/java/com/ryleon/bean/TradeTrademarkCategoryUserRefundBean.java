package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * <p>交易域品牌-品类-用户粒度退单各窗口汇总表</p>
 *
 * @author ALiang
 * @date 2023-01-06
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserRefundBean {
    /**
     * 窗口起始时间
     */
    String stt;
    /**
     * 窗口结束时间
     */
    String edt;
    /**
     * 品牌 ID
     */
    String trademarkId;
    /**
     * 品牌名称
     */
    String trademarkName;
    /**
     * 一级品类 ID
     */
    String category1Id;
    /**
     * 一级品类名称
     */
    String category1Name;
    /**
     * 二级品类 ID
     */
    String category2Id;
    /**
     * 二级品类名称
     */
    String category2Name;
    /**
     * 三级品类 ID
     */
    String category3Id;
    /**
     * 三级品类名称
     */
    String category3Name;

    /**
     * 订单 ID
     */
    @TransientSink
    Set<String> orderIdSet;

    /**
     * sku_id
     */
    @TransientSink
    String skuId;

    /**
     * 用户 ID
     */
    String userId;
    /**
     * 退单次数
     */
    Long refundCount;
    /**
     * 时间戳
     */
    Long ts;

}
