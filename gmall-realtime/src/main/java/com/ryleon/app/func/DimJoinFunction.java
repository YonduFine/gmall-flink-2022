package com.ryleon.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author ALiang
 * @date 2023-01-06
 * @effect 维表信息关联
 */
public interface DimJoinFunction<T> {

    /**
     * 根据Key查询Hbase表中的数据
     *
     * @param input 待关联的JavaBean
     * @return String
     */
    String getKey(T input);

    /**
     * 关联维度信息
     *
     * @param input   主表数据
     * @param dimInfo 待关联的的维度信息
     */
    void join(T input, JSONObject dimInfo);
}
