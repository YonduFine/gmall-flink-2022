package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.UserLoginBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author ALiang
 * @date 2023-01-02
 * @effect 用户域用户登陆各窗口汇总表--统计七日回流用户和当日独立用户数
 *
 * <p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Clickhouse
 * <p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwsUserUserLoginWindow->Clickhouse
 */
public class DwsUserUserLoginWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsUserUserLoginWindow();
        driver.execute("DwsUserUserLoginWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka DWD页面日志数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String groupId = StrUtil.toUnderlineCase("DwsUserUserLoginWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.将数据过滤并转换为JSONObject
        SingleOutputStreamOperator<JSONObject> filterDs = kafkaDs.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                String uid = common.getString("uid");
                JSONObject page = jsonObject.getJSONObject("page");
                String lastPageId = page.getString("last_page_id");
                String loginStr = "login";
                if (uid != null && lastPageId == null || loginStr.equals(lastPageId)) {
                    out.collect(jsonObject);
                }
            }
        });

        // todo 3.抽取WaterMark
        SingleOutputStreamOperator<JSONObject> assignWmDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // todo 4.根据user_id进行分组
        KeyedStream<JSONObject, String> keyedStream = assignWmDs.keyBy(json -> json.getJSONObject("common").getString("uid"));

        // todo 5.使用状态变量过滤回流用户和UV用户
        AllWindowedStream<UserLoginBean, TimeWindow> backUniqueDs = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> uvDescriptor = new ValueStateDescriptor<>("last-login-state", String.class);

                lastLoginState = getRuntimeContext().getState(uvDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                // 获取当前状态和数据中日期
                Long ts = value.getLong("ts");
                String currentLoginDt = DateFormatUtil.toDate(ts);
                String lastLoginDt = lastLoginState.value();

                // 定义回流用户度量和独立用户度量
                long backCt = 0L;
                long uvCt = 0L;

                // 判断是否存在上次登陆
                if (lastLoginDt != null) {
                    // 判断上次是登陆日期是否为当日
                    if (!currentLoginDt.equals(lastLoginDt)) {
                        uvCt = 1L;
                        long days = (ts - DateFormatUtil.toTs(lastLoginDt, false)) / 1000 / 24 / 3600;
                        // 如果上次登陆日期距本次登陆大于7天则为回流用户
                        backCt = days > 7 ? 1L : 0L;
                        lastLoginState.update(currentLoginDt);
                    }
                } else {
                    uvCt = 1L;
                    lastLoginState.update(currentLoginDt);
                }

                if (uvCt != 0L || backCt != 0L) {
                    out.collect(new UserLoginBean("", "", backCt, uvCt, ts));
                }
            }
        }).windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // todo 6.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDs = backUniqueDs.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean next = values.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        resultDs.print(">>");

        // todo 7.将结果写入ClickHouse
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_user_user_login_window values(?,?,?,?,?)"
        ));
    }
}
