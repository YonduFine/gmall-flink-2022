package com.ryleon.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)
 <p>
程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 设置检查点
        // env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //1.2 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.3设置用户
        // System.setProperty("HADOOP_USER_NAME","ryl");

        // 2.消费Kafka topic_log主题数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.log.topic.name");
        String groupId = properties.getProperty("dwd.kafka.log.group_id.name");
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        // todo 3.过滤数据 & 将数据封装为JOSNObject
        OutputTag<String> dirtyDS = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 将不合法数据写出到侧输出流
                    ctx.output(dirtyDS, value);
                }
            }
        });
        // 输出不合法数据
        filterDS.getSideOutput(dirtyDS).print("非法数据>>>>>");

        // todo 4.按照mid进行分组
        // {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_77821","os":"iOS 13.2.3","uid":"853","vc":"v2.1.134"},"start":{"entry":"icon","loading_time":2040,"open_ad_id":19,"open_ad_ms":3308,"open_ad_skip_ms":0},"ts":1645423406000}
        KeyedStream<JSONObject, String> keyedStream = filterDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        // todo 5.对新老用户进行修复 isNew
        SingleOutputStreamOperator<JSONObject> receiveIsNewDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<String>("lastDate", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstViewDt = lastValueState.value();
                Long ts = value.getLong("ts");
                String currentDt = DateFormatUtil.toDate(ts);
                if ("1".equals(isNew)) {
                    if (firstViewDt == null) {
                        lastValueState.update(currentDt);
                    } else if (!firstViewDt.equals(currentDt)) {
                        common.put("is_new", "0");
                    }
                } else if (lastValueState == null) {
                    lastValueState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });

        // todo 6.将数据按照主题写入侧输出流
        OutputTag<String> errOutputTag = new OutputTag<String>("error") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionOutputTag = new OutputTag<String>("action") {
        };
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        SingleOutputStreamOperator<String> pageDS = receiveIsNewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    ctx.output(errOutputTag, value.toJSONString());
                }
                // 剔除错误信息
                value.remove("err");

                JSONObject start = value.getJSONObject("start");
                JSONArray display = value.getJSONArray("displays");
                JSONArray action = value.getJSONArray("actions");
                JSONObject common = value.getJSONObject("common");
                JSONObject page = value.getJSONObject("page");
                Long ts = value.getLong("ts");
                if (start != null) {
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    if (display != null && display.size() > 0) {
                        JSONObject displayJsonObj = new JSONObject();
                        displayJsonObj.put("common", common);
                        displayJsonObj.put("displays", display);
                        displayJsonObj.put("page", page);
                        displayJsonObj.put("ts", ts);
                        ctx.output(displayOutputTag, displayJsonObj.toJSONString());
                    }
                    if (action != null && action.size() > 0) {
                        JSONObject actionJsonObj = new JSONObject();
                        actionJsonObj.put("common", common);
                        actionJsonObj.put("actions", action);
                        actionJsonObj.put("page", page);
                        ctx.output(actionOutputTag, actionJsonObj.toJSONString());
                    }
                    value.remove("displays");
                    value.remove("actions");

                    out.collect(value.toJSONString());
                }
            }
        });

        // todo 7.获取侧输出流数据 & 打印输出
        DataStream<String> errDS = pageDS.getSideOutput(errOutputTag);
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionOutputTag);
        errDS.print("ERROR >>>");
        startDS.print("START <<<");
        displayDS.print("DISPLAY @@@");
        actionDS.print("ACTION ###");
        pageDS.print("PAGE $$$");

        // todo 8.将数写入对应Kafka topic
        String errTopic = properties.getProperty("dwd.kafka.traffic_error_log.topic");
        String startTopic = properties.getProperty("dwd.kafka.traffic_start_log.topic");
        String displayTopic = properties.getProperty("dwd.kafka.traffic_display_log.topic");
        String actionTopic = properties.getProperty("dwd.kafka.traffic_action_log.topic");
        String pageTopic = properties.getProperty("dwd.kafka.traffic_page_log.topic");

        errDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(errTopic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(startTopic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(displayTopic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(actionTopic));
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(pageTopic));

        // todo 9.启动程序
        env.execute("BaseLogApp");
    }
}
