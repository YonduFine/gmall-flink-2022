package com.ryleon.app.dwd.log;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Lenovo
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 设置检查点
        // env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        // 1.2 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.3 设置用户
        // System.setProperty("HADOOP_USER_NAME","ryl");

        // todo 2.消费kafka topic数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String groupId = "dwd_traffic_userJump_detail_2212";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3.转化数据格式为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaDs.map(JSONObject::parseObject);

        // todo 4.分配水位线 & 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream
            .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(line -> line.getJSONObject("common").getString("mid"));

        // todo 5.定义匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    return value.getJSONObject("page").getString("last_page_id") == null;
                }
            }).times(2)
            .consecutive() // 宽松连续
            .within(Time.seconds(10));

        // todo 6.将规则应用到流中
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // todo 7.从流中提取符合规则和超时数据
        /*
        规则一：last_page_id == null 则返回 true 发生跳出行为的必然是会话起始页面，上页 id 必然为 null，舍弃了非会话起始页的日志数据
        规则二：last_page_id == null 则返回 true 规则二和规则一之间的连续策略为严格连续。如果两条数据匹配了规则一和规则二，则满足规则一的数据为跳出明细数据。
        指定超时时间 超时时间内第二条数据没有到来时，第一条数据是目标数据，被判定为跳出明细数据
        */
        OutputTag<JSONObject> timoutTag = new OutputTag<JSONObject>("timeout") {
        };
        SingleOutputStreamOperator<JSONObject> selectStream = patternStream.flatSelect(timoutTag,
            new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                @Override
                public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                    JSONObject timeoutData = pattern.get("start").get(0);
                    out.collect(timeoutData);
                }
            },
            new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                @Override
                public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                    JSONObject jumpData = pattern.get("start").get(0);
                    out.collect(jumpData);
                }
            });

        // todo 8.合并数据
        DataStream<JSONObject> timeoutStream = selectStream.getSideOutput(timoutTag);
        timeoutStream.print("TimeOut >>>");
        selectStream.print("Select >>>");
        DataStream<JSONObject> dataStream = selectStream.union(timeoutStream);

        // todo 9.将数据写如Kafka对应的topic中
        String userJumpTopic = properties.getProperty("dwd.kafka.traffic_uj_detail.topic");
        dataStream
            .map(JSONAware::toJSONString)
            .addSink(MyKafkaUtil.getFlinkKafkaProducer(userJumpTopic));

        // todo 10.启动程序
        env.execute("DwdTrafficUserJumpDetail");
    }
}
