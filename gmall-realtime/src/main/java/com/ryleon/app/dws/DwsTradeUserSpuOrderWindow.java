package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.app.func.DimAsyncFunction;
import com.ryleon.bean.TradeUserSpuOrderBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * <p>SPU粒度下单个窗口汇总表</p>
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_pre_Process)->FlinkApp->KafkaFlinkApp->ClikHouse
 * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderPreProcess->Kafka(ZK)->DwdTradeOrderDetail->Kafka(ZK)->DwsTradeUserSpuOrderWindow(Hbase(HDFS\Phoenix\ZK、Redis))->ClikHouse
 * [若有新用户产生时或维表会发生变化时，测试时需要启动DimApp]
 *
 * @author ALiang
 * @date 2023-01-04
 */
public class DwsTradeUserSpuOrderWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradeUserSpuOrderWindow();
        driver.execute("DwsTradeUserSpuOrderWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka dwd下单主题数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_detail.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradeUserSpuOrderWindow");
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
        KeyedStream<JSONObject, String> keyedByDetailIdStream = jsonObjDs.keyBy(json -> json.getString("id"));

        // todo 4.按照order_detail_id保留第一条数据
        SingleOutputStreamOperator<JSONObject> filterDs = keyedByDetailIdStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        // todo 5.封装为JavaBean
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuBeanDs = filterDs.map(json -> {
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(json.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                .skuId(json.getString("sku_id"))
                .userId(json.getString("user_id"))
                .orderIdSet(orderIdSet)
                .orderAmount(json.getDouble("split_total_amount"))
                .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                .build();
        });
        tradeUserSpuBeanDs.print("tradeSpu>>");

        // todo 6.关联sku_info表 补充spu_id,tm_id,category3_id (Async I/O)
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserWithSpuDs = AsyncDataStream.unorderedWait(tradeUserSpuBeanDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                input.setSpuId(dimInfo.getString("SPU_ID"));
                input.setTrademarkId(dimInfo.getString("TM_ID"));
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
            }
        }, 150, TimeUnit.SECONDS);
        tradeUserWithSpuDs.print("tradeSpuWithSpu>>");

        // todo 7.分配水位线
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWmDs = tradeUserWithSpuDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
            @Override
            public long extractTimestamp(TradeUserSpuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // todo 8.按照user_id\tm_id\spu_id\category3_id分组 开窗 聚合
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultDs = tradeUserSpuWmDs.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean value) throws Exception {
                    return Tuple4.of(value.getUserId(), value.getSpuId(), value.getTrademarkId(), value.getCategory3Id());
                }
            }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
            .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                @Override
                public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                    value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    return value1;
                }
            }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                @Override
                public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TradeUserSpuOrderBean> input, Collector<TradeUserSpuOrderBean> out) throws Exception {
                    TradeUserSpuOrderBean spuOrderBean = input.iterator().next();
                    spuOrderBean.setOrderCount((long) spuOrderBean.getOrderIdSet().size());
                    spuOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    spuOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    spuOrderBean.setTs(System.currentTimeMillis());
                    out.collect(spuOrderBean);
                }
            });

        // todo 9.补充其他维度信息
        // 9.1 补充spu_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithSpuNameDs = AsyncDataStream.unorderedWait(resultDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                @Override
                public String getKey(TradeUserSpuOrderBean input) {
                    return input.getSpuId();
                }

                @Override
                public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                    input.setSpuName(dimInfo.getString("SPU_NAME"));
                }
            },
            150, TimeUnit.SECONDS);
        // 9.2 补充tm_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithTmNameDs = AsyncDataStream.unorderedWait(resultWithSpuNameDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                @Override
                public String getKey(TradeUserSpuOrderBean input) {
                    return input.getTrademarkId();
                }

                @Override
                public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                    input.setTrademarkName(dimInfo.getString("TM_NAME"));
                }
            },
            150, TimeUnit.SECONDS);
        // 9.3 补充category3_name,category2_id
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithCate3Ds = AsyncDataStream.unorderedWait(resultWithTmNameDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                @Override
                public String getKey(TradeUserSpuOrderBean input) {
                    return input.getCategory3Id();
                }

                @Override
                public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                    input.setCategory3Name(dimInfo.getString("NAME"));
                    input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                }
            },
            150, TimeUnit.SECONDS);
        // 9.4 补充category2_name,category1_id
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithCate2Ds = AsyncDataStream.unorderedWait(resultWithCate3Ds, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                @Override
                public String getKey(TradeUserSpuOrderBean input) {
                    return input.getCategory2Id();
                }

                @Override
                public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                    input.setCategory2Name(dimInfo.getString("NAME"));
                    input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                }
            },
            150, TimeUnit.SECONDS);
        // 9.5 补充category1_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithCate1Ds = AsyncDataStream.unorderedWait(resultWithCate2Ds, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                @Override
                public String getKey(TradeUserSpuOrderBean input) {
                    return input.getCategory1Id();
                }

                @Override
                public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                    input.setCategory1Name(dimInfo.getString("NAME"));
                }
            },
            150, TimeUnit.SECONDS);

        // todo 10.将数据写出到Clickhouse
        resultWithCate1Ds.print(">>>>>");
        resultWithCate1Ds.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));
    }
}
