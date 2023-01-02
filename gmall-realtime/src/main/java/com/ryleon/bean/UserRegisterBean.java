package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author ALiang
 * @date 2023-01-02
 * @effect 用户域用户注册各窗口汇总表Bean
 */

@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
