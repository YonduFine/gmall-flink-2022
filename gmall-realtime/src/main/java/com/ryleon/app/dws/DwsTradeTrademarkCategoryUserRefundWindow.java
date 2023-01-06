package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.app.func.DimAsyncFunction;
import com.ryleon.bean.TradeTrademarkCategoryUserRefundBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * <p>交易域品牌-品类-用户粒度退单各窗口汇总表</p>
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_refund)->FlinkApp->Clickhouse
 * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderRefund->Kafka(ZK)->DwsTradeTrademarkCategoryUserRefundWindow(Hbase(HDFS)/Redis)->Clickhouse
 * [若有新用户产生时或维表会发生变化时，测试时需要启动DimApp]
 *
 * @author ALiang
 * @date 2023-01-06
 */
public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseDwsFlinkApp {


    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradeTrademarkCategoryUserRefundWindow();
        driver.execute("DwsTradeTrademarkCategoryUserRefundWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka dwd退单主题
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_refund.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradeTrademarkCategoryUserRefundWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.数据格式转
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

        // todo 3.封装为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeBeanDs = jsonObjDs.map(json -> {
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(json.getString("order_id"));
            return TradeTrademarkCategoryUserRefundBean.builder()
                .userId(json.getString("user_id"))
                .skuId(json.getString("sku_id"))
                .orderIdSet(orderIdSet)
                .ts(json.getLong("create_time"))
                .build();
        });

        // todo 4.关联维度数据 dim_sku_info
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeSkuDs = AsyncDataStream.unorderedWait(tradeBeanDs, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setTrademarkId(dimInfo.getString("TM_ID"));
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
            }
        }, 100, TimeUnit.SECONDS);

        // todo 5.分配水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeSkuWmDs = tradeSkuDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // todo 6.分组 开窗 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultDs = tradeSkuWmDs.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
                @Override
                public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                    return Tuple3.of(value.getUserId(), value.getTrademarkId(), value.getCategory3Id());
                }
            }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    return value1;
                }
            }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                @Override
                public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                    TradeTrademarkCategoryUserRefundBean categoryUserRefundBean = input.iterator().next();
                    categoryUserRefundBean.setRefundCount((long) categoryUserRefundBean.getOrderIdSet().size());
                    categoryUserRefundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    categoryUserRefundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    categoryUserRefundBean.setTs(System.currentTimeMillis());

                    out.collect(categoryUserRefundBean);
                }
            });

        // todo 7.补充字段信息
        // 7.1 补充tm_name
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultTmNameDs = AsyncDataStream.unorderedWait(resultDs, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setTrademarkName(dimInfo.getString("TM_NAME"));
            }
        }, 150, TimeUnit.SECONDS);

        // 7.2 补充category3_name,category2_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultCate3Ds = AsyncDataStream.unorderedWait(resultTmNameDs, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory3Name(dimInfo.getString("NAME"));
                input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
            }
        }, 150, TimeUnit.SECONDS);

        // 7.3 补充category2_name,category1_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultCate2Ds = AsyncDataStream.unorderedWait(resultCate3Ds, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory2Name(dimInfo.getString("NAME"));
                input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
            }
        }, 150, TimeUnit.SECONDS);

        // 7.4 补充category1_name
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultCate1Ds = AsyncDataStream.unorderedWait(resultCate2Ds, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory1Name(dimInfo.getString("NAME"));
            }
        }, 150, TimeUnit.SECONDS);

        // todo 8.写出结果到Clickhouse
        resultCate1Ds.print(">>>>>");
        resultCate1Ds.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));
    }
}
