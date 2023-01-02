package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author ALiang
 * @date 2023-01-02
 * @effect 交易域加购各窗口汇总表-统计每日各窗口加购独立用户数Bean
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;
}
