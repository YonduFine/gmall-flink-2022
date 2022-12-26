package com.ryleon.app.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.validation.constraints.NotNull;

/**
 * @author ALiang
 * @date 2022-12-25
 * @effect 描述该类的作用
 */
public interface IProcess {

    /**
     * 程序执行
     *
     * @param appName 应用名称
     * @throws Exception 异常信息
     */
    void execute(@NotNull String appName) throws Exception;

    /**
     * 业务逻辑编写
     *
     * @param env 执行环境
     * @param tableEnv FlinkSQL执行环境
     * @throws Exception 异常
     */
    void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception;

}
