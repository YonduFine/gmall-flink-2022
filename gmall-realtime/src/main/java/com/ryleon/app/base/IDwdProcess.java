package com.ryleon.app.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.validation.constraints.NotNull;

/**
 * @author ALiang
 * @date 2022-12-23
 */
public interface IDwdProcess extends IProcess{

    /**
     * 程序执行
     *
     * @param appName 应用名称
     * @param ttl 设置状态超时时间
     * @throws Exception 异常信息
     */
    void execute(@NotNull String appName,Integer ttl) throws Exception;

    /**
     * 读取数据
     * 为后续处理提供数据
     *
     * @param env 执行环境
     * @param tableEnv FlinkSQL执行环境
     * @param appName 应用程序名称
     * @throws Exception 异常
     */
    void loadData(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv,String appName) throws Exception;

    /**
     * 是否执行env.execute()
     *
     * @param env Flink 执行环境
     * @param appName 应用程序名称
     * @throws Exception 异常
     */
    void startEnv(StreamExecutionEnvironment env, String appName) throws Exception;
}
