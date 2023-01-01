package com.ryleon.app.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author ALiang
 * @date 2023-01-01
 * @effect DWS层环境基类
 */
public abstract class BaseDwsFlinkApp implements IProcess{
    @Override
    public void execute(String appName) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 创建Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        // 1.2 设置检查点
        // env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        // 1.3 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.4 设置用户信息
        // System.setProperty("HADOOP_USER_NAME","ryl");

        // 业务处理
        process(env,tableEnv);

        env.execute(appName);
    }

}
