package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.TrafficHomeDetailPageViewBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * @author ALiang
 * @date 2023-01-01
 * @effect 流量域页面浏览各窗口汇总表--统计当日的首页和商品详情页独立访客数
 *
 * <p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Clickhouse
 * <p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwsTrafficPageViewWindow->Clickhouse
 */
public class DwsTrafficPageViewWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTrafficPageViewWindow();
        driver.execute("DwsTrafficPageViewWindow");

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费页面浏览数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTrafficPageViewWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.过滤出出访问主页合商品详情页的数据并转换为JSONObject
        SingleOutputStreamOperator<JSONObject> filterDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                String home = "home";
                String goodDetail = "good_detail";
                if (home.equals(pageId) || goodDetail.equals(pageId)) {
                    out.collect(jsonObject);
                }
            }
        });

        // todo 3.抽取WaterMark
        SingleOutputStreamOperator<JSONObject> assignWatermarksDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // todo 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = assignWatermarksDs.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // todo 5.使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> homeDetailPageViewDs = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeState;
            private ValueState<String> goodDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 配置TTL更新策略
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
                ValueStateDescriptor<String> homeValueDesc = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> goodDetailStateDesc = new ValueStateDescriptor<>("good-detail-state", String.class);

                // 设置TTL
                homeValueDesc.enableTimeToLive(ttlConfig);
                goodDetailStateDesc.enableTimeToLive(ttlConfig);

                homeState = getRuntimeContext().getState(homeValueDesc);
                goodDetailState = getRuntimeContext().getState(goodDetailStateDesc);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 获取当前状态中的数据和数据中的日期
                String homeLastDt = homeState.value();
                String goodDetailLastDt = goodDetailState.value();
                Long ts = value.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                String home = "home";
                long homeCt = 0L;
                long goodDetailCt = 0L;
                //如果状态为空或上次访问日期与当前日期不同，则为需要数据
                if (home.equals(value.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDt == null || !homeLastDt.equals(currentDate)) {
                        homeCt = 1L;
                        homeState.update(currentDate);
                    }
                } else {
                    if (goodDetailLastDt == null || !goodDetailLastDt.equals(currentDate)) {
                        goodDetailCt = 1L;
                        goodDetailState.update(currentDate);
                    }
                }
                // 满足任何一个不等于0的即可写出
                if (homeCt == 1L || goodDetailCt == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, goodDetailCt, ts));
                }
            }
        });

        // todo 6.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDs = homeDetailPageViewDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
            .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                @Override
                public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                    value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                    value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                    return value1;
                }
            }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    TrafficHomeDetailPageViewBean next = values.iterator().next();
                    next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    next.setTs(System.currentTimeMillis());
                    out.collect(next);
                }
            });
        resultDs.print(">>");

        // todo 7.将数据写入ClickHouse
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
        ));
    }
}
