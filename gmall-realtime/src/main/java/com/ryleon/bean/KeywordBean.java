package com.ryleon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ALiang
 * @date 2022-12-31
 * @effect 关键词统计Bean（实体Bean中的字段名必须与表字段保持一致）
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    /**
     * 窗口起始时间
     */
    private String stt;
    /**
     * 窗口闭合时间
     */
    private String edt;
    /**
     * 关键词来源
     */
    private String source;
    /**
     * 关键词
     */
    private String keyword;
    /**
     * 关键词出现频次
     */
    private Long keyword_count;
    /**
     * 时间戳
     */
    private Long ts;
}

