package com.ryleon.app.base;

import cn.hutool.core.util.StrUtil;
import com.ryleon.util.MyKafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author ALiang
 * @date 2022-12-23
 */
public abstract class BaseDwdFlinkApp implements IDwdProcess {

    @Override
    public void execute(String appName) throws Exception {
        execute(appName, null);
    }

    @Override
    public void execute(String appName, Integer ttl) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 创建Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        if (ttl != null) {
            // 获取配置对象
            Configuration configuration = tableEnv.getConfig().getConfiguration();
            // 为表关联时状态中存储的数据设置过期时间
            configuration.setString("table.exec.state.ttl", ttl + " s");
        }

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

        // todo 2.执行程序逻辑
        // 1.消费Kafka topic数据并封装为Flink SQL表
        loadData(env, tableEnv, appName);
        process(env, tableEnv);

        // todo 3.启动程序
        startEnv(env, appName);
    }

    @Override
    public void loadData(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String appName) throws Exception {
        //  将应用程序名转换为groupId
        String groupId = StrUtil.toUnderlineCase(appName);
        String topicDb = MyKafkaUtil.getTopicDb(groupId);
        tableEnv.executeSql(topicDb);
    }
}
