package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.app.func.DimAsyncFunction;
import com.ryleon.bean.TradeProvinceOrderWindowBean;
import com.ryleon.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * <p>交易域省份粒度下单各窗口汇总表 统计各省份各窗口订单数和订单金额</p>
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_pre_Process)->FlinkApp->KafkaFlinkApp->ClikHouse
 *  * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderPreProcess->Kafka(ZK)->DwdTradeOrderDetail->Kafka(ZK)->DwsTradeProvinceOrderWindow(Hbase(HDFS\Phoenix\ZK、Redis))->ClikHouse
 *  * [若有新用户产生时或维表会发生变化时，测试时需要启动DimApp]
 *
 * @author ALiang
 * @date 2023-01-06
 */
public class DwsTradeProvinceOrderWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradeProvinceOrderWindow();
        driver.execute("DwsTradeProvinceOrderWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka dwd下单主题数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_detail.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradeProvinceOrderWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.数据格式转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("异常数据：" + value);
                }
            }
        });

        // todo 3.按照order_detail_id分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdDs = jsonObjDs.keyBy(json -> json.getString("id"));

        // todo 4.数据去重（取最后一条数据）
        SingleOutputStreamOperator<JSONObject> filterDs = keyedByOrderDetailIdDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("last-order", JSONObject.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastOrder = valueState.value();
                if (lastOrder == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                } else {
                    String lastDt = lastOrder.getString("row_op_ts");
                    String currentDt = value.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(lastDt, currentDt);
                    if (compare != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject value = valueState.value();
                valueState.clear();
                out.collect(value);
            }
        });
        // todo 5.封装为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> tradeProvinceDs = filterDs.map(line -> {
            HashSet<String> orderSet = new HashSet<>();
            orderSet.add(line.getString("order_id"));
            return TradeProvinceOrderWindowBean.builder()
                .provinceId(line.getString("province_id"))
                .orderIdSet(orderSet)
                .orderAmount(line.getDouble("split_total_amount"))
                .ts(DateFormatUtil.toTs(line.getString("create_time"), true))
                .build();
        });

        // todo 6.抽取Watermark
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> tradeProvinceWmDs = tradeProvinceDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindowBean>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // todo 7.分组 开窗 聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> resultDs = tradeProvinceWmDs.keyBy(TradeProvinceOrderWindowBean::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<TradeProvinceOrderWindowBean>() {
                @Override
                public TradeProvinceOrderWindowBean reduce(TradeProvinceOrderWindowBean value1, TradeProvinceOrderWindowBean value2) throws Exception {
                    value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    return value1;
                }
            }, new WindowFunction<TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindowBean> input, Collector<TradeProvinceOrderWindowBean> out) throws Exception {
                    TradeProvinceOrderWindowBean provinceOrderWindowBean = input.iterator().next();
                    provinceOrderWindowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    provinceOrderWindowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    provinceOrderWindowBean.setOrderCount((long) provinceOrderWindowBean.getOrderIdSet().size());
                    provinceOrderWindowBean.setTs(System.currentTimeMillis());
                    out.collect(provinceOrderWindowBean);
                }
            });

        // todo 8.补充字段信息
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> resultProvinceDs = AsyncDataStream.unorderedWait(resultDs, new DimAsyncFunction<TradeProvinceOrderWindowBean>("DIM_BASE_PROVINCE") {
                @Override
                public String getKey(TradeProvinceOrderWindowBean input) {
                    return input.getProvinceId();
                }

                @Override
                public void join(TradeProvinceOrderWindowBean input, JSONObject dimInfo) {
                    input.setProvinceName(dimInfo.getString("NAME"));
                }
            }
            , 150, TimeUnit.SECONDS);

        // todo 9.将数据写出到Clickhouse
        resultProvinceDs.print("provinceName>>>");
        resultProvinceDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"
        ));
    }
}
