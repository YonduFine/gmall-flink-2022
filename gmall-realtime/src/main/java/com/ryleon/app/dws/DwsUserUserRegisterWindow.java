package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.UserRegisterBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * @author ALiang
 * @date 2023-01-02
 * @effect 用户域用户注册各窗口汇总表
 *
 * <p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Clickhouse
 * <p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwsUserUserRegisterWindow->Clickhouse
 */
public class DwsUserUserRegisterWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsUserUserRegisterWindow();
        driver.execute("DwsUserUserRegisterWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费kafka DWD用户注册数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.user_register.topic");
        String groupId = StrUtil.toUnderlineCase("DwsUserUserRegisterWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.将数据封装为JavaBean
        SingleOutputStreamOperator<UserRegisterBean> userRegDs = kafkaDs.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String createTime = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(createTime, true));
        });

        // todo 3.分配水位线
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDs = userRegDs.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // todo 4.开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDs = userRegisterDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<UserRegisterBean>() {
                @Override
                public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                    value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                    return value1;
                }
            }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                    UserRegisterBean next = values.iterator().next();
                    next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    next.setTs(System.currentTimeMillis());
                    out.collect(next);
                }
            });

        resultDs.print(">>");
        // todo 5.将数据写入Clickhouse
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_user_user_register_window values(?,?,?,?)"
        ));
    }
}
