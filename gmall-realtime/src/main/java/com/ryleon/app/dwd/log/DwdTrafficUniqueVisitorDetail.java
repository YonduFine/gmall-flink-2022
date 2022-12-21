package com.ryleon.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author RYL
 * <p>
 * {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_77821","os":"iOS 13.2.3","uid":"853","vc":"v2.1.134"},"page":{"during_time":11911,"page_id":"home"},"ts":1645423406000}
 * <p>
 * 数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Kafka(DWD_UV)
 *  <p>
 * 程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUniqueVisitorDetail->Kafka(ZK)
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //1.2 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.3 设置用户
        // System.setProperty("HADOOP_USER_NAME","ryl");

        // todo 2.读取Kafka Topic数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String groupId = "dwd_traffic_uv_detail_2212";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3.过滤lastPageId不为null的数据 & 转换数格式为JSONObject
        SingleOutputStreamOperator<JSONObject> mappedStream = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null){
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("非法数据 >> "+ value);
                }
            }
        });

        // todo 4.按照Mid将数据分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // todo 5.使用状态编程过滤独立访客
        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<String>("last-visit", String.class);
                // 设置TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
                descriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                Long ts = value.getLong("ts");
                String currentDt = DateFormatUtil.toDate(ts);
                String lastVisitDt = lastVisitState.value();
                if (!currentDt.equals(lastVisitDt)) {
                    lastVisitState.update(currentDt);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // todo 6.将数据写入Kafka Topic
        String uvDetailTopic = properties.getProperty("dwd.kafka.traffic_uv_detail.topic");
        filterStream
            .map(new RichMapFunction<JSONObject, String>() {
                @Override
                public String map(JSONObject value) throws Exception {
                    return value.toJSONString();
                }
            })
            .addSink(MyKafkaUtil.getFlinkKafkaProducer(uvDetailTopic));

        // todo 7.执行程序
        env.execute();
    }
}
