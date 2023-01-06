package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.TradeOrderBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Properties;

/**
 * @author ALiang
 * @date 2023-01-04
 * @effect 交易域下单各窗口汇总表--统计当日下单独立用户数和新增下单用户数
 *
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_pre_Process)->FlinkApp->Kafka->FlinkApp->Clickhouse(dws)
 * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderPreProcess->Kafka(ZK)->DwdTradeOrderDetail->Kafka(ZK)->DwsTradeOrderWindow->Clickhouse
 */
public class DwsTradeOrderWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradeOrderWindow();
        driver.execute("DwsTradeOrderWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费kafka dwd 下单主题数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_detail.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradeOrderWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.过滤并转换数据格式为JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("非法格式：" + value);
                }
            }
        });

        // todo 3.按照order_detail_id分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjDs.keyBy(line -> line.getString("id"));

        // todo 4.去重订单数据
        SingleOutputStreamOperator<JSONObject> filterDs = keyedByOrderDetailIdStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String isExists = valueState.value();
                if (isExists == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        // todo 5.抽取水位线
        SingleOutputStreamOperator<JSONObject> assignWmDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return DateFormatUtil.toTs(element.getString("create_time"), true);
            }
        }));

        // todo 6.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = assignWmDs.keyBy(line -> line.getString("user_id"));

        // todo 7.使用状态编程计算独立下单用户
        SingleOutputStreamOperator<TradeOrderBean> mappedDs = keyedByUserIdStream.map(new RichMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastValue;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-order-dt", String.class);
                lastValue = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public TradeOrderBean map(JSONObject value) throws Exception {
                String lastDt = lastValue.value();
                String currentDt = value.getString("create_time");

                long uniqueOrderUserCt = 0L;
                long newOrderUserCt = 0L;
                // 判断是否存在上次下单，若不存在则为新用户首次下单
                if (lastDt == null) {
                    uniqueOrderUserCt = 1L;
                    newOrderUserCt = 1L;
                } else if (!currentDt.equals(lastDt)) {
                    uniqueOrderUserCt = 1L;
                }

                Double splitActivityAmount = value.getDouble("split_activity_amount");
                Double splitCouponAmount = value.getDouble("split_coupon_amount");
                Integer skuNum = value.getInteger("sku_num");
                Double orderPrice = value.getDouble("order_price");
                splitActivityAmount = splitActivityAmount != null ? splitActivityAmount : 0.0;
                splitCouponAmount = splitCouponAmount != null ? splitCouponAmount : 0.0;
                BigDecimal orderOriginalPrice = BigDecimal.valueOf(orderPrice).multiply(BigDecimal.valueOf(skuNum));

                return new TradeOrderBean("", "",
                    uniqueOrderUserCt,
                    newOrderUserCt,
                    BigDecimal.valueOf(splitActivityAmount),
                    BigDecimal.valueOf(splitCouponAmount),
                    orderOriginalPrice,
                    null);
            }
        });

        // todo 8.开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDs = mappedDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
            .reduce(new ReduceFunction<TradeOrderBean>() {
                @Override
                public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                    value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                    value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                    value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount()
                            .add(value2.getOrderActivityReduceAmount()));
                    value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount()
                            .add(value2.getOrderCouponReduceAmount()));
                    value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount()
                            .add(value2.getOrderOriginalTotalAmount()));
                    return value1;
                }
            }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                    TradeOrderBean orderBean = values.iterator().next();
                    orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    orderBean.setTs(System.currentTimeMillis());
                    out.collect(orderBean);
                }
            });

        // todo 9.将计算结果写入Clickhouse
        resultDs.print(">>");
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"
        ));
    }
}
