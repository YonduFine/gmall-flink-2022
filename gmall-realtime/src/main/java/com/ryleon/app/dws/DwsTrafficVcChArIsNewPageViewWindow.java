package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.TrafficPageViewBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * @author ALiang
 * @date 2023-01-01
 * @effect 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 *
 *<p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD_page)->FlinkApp
 *<p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Kafka(DWD_UV)->FlinkApp
 *<p>数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Kafka(DWD_UJ)->FlinkApp
 * <p>//// -> DwsTrafficVcChArIsNewPageViewWindow->Clickhouse
 *<p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)-
 *<p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUniqueVisitorDetail->Kafka(ZK)
 *<p>程序：mock->Flume->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUserJumpDetail->Kafka(ZK)
 * <p>//// -> DwsTrafficVcChArIsNewPageViewWindow->Clickhouse
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTrafficVcChArIsNewPageViewWindow();
        driver.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.获取页面、用户跳出、独立访客三条流数据并封装统一数据格式
        // 1.1 消费数据
        Properties properties = PropertiesUtil.getProperties();
        String pageTopic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String ujTopic = properties.getProperty("dwd.kafka.traffic_uj_detail.topic");
        String uvTopic = properties.getProperty("dwd.kafka.traffic_uv_detail.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTrafficVcChArIsNewPageViewWindow");

        DataStreamSource<String> pageDataStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));
        DataStreamSource<String> ujDataStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujTopic, groupId));
        DataStreamSource<String> uvDataStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));

        // 1.2统一数据格式
        SingleOutputStreamOperator<TrafficPageViewBean> pageViewDs = pageDataStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            Long ts = jsonObject.getLong("ts");
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");
            Long duringTime = page.getLong("during_time");
            String lastPageId = page.getString("last_page_id");
            long sv = lastPageId == null ? 1L : 0L;
            return new TrafficPageViewBean("", "", vc, ch, ar, isNew, 0L, sv, 1L, duringTime, 0L, ts);
        });

        SingleOutputStreamOperator<TrafficPageViewBean> userJumpDs = ujDataStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            Long ts = jsonObject.getLong("ts");
            JSONObject common = jsonObject.getJSONObject("common");
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");
            return new TrafficPageViewBean("", "", vc, ch, ar, isNew, 0L, 0L, 0L, 0L, 1L, ts);
        });

        SingleOutputStreamOperator<TrafficPageViewBean> uniqueVisitorDs = uvDataStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            Long ts = jsonObject.getLong("ts");
            JSONObject common = jsonObject.getJSONObject("common");
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");
            return new TrafficPageViewBean("", "", vc, ch, ar, isNew, 1L, 0L, 0L, 0L, 0L, ts);
        });

        // todo 2.三流合并
        DataStream<TrafficPageViewBean> unionDs = pageViewDs.union(userJumpDs, uniqueVisitorDs);

        // todo 3.分配WaterMark
        SingleOutputStreamOperator<TrafficPageViewBean> pageViewWmDs = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // todo 4.按照维度分组开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = pageViewWmDs.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return Tuple4.of(value.getCh(), value.getVc(), value.getAr(), value.getIsNew());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // todo 5.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> resultDs = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                // 由于经过前一阶段的聚合 这里只有一条数据
                TrafficPageViewBean next = input.iterator().next();
                // 补充窗口开始合结束时间
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                // 更新当前处理时间
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        resultDs.print(">>");

        // todo 6.将计算结果写入Clickhouse
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
            ));
    }
}
