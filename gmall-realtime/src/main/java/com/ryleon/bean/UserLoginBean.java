package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author ALiang
 * @date 2023-01-02
 * @effect 用户登陆--回流用户和独立访客Bean
 */
@Data
@AllArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;

}
