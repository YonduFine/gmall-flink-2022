package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.TradePaymentWindowBean;
import com.ryleon.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
 * @date 2023-01-03
 * @effect 交易域支付各窗口汇总表--统计支付成功独立用户数和首次支付成功用户数
 *
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_pay_detail_suc)->FlinkApp->Clickhouse(DWS)
 * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradePayDetailSuc->Kafka(ZK)>DwsTradePaymentSucWindow->Clickhouse(DWS)
 */
public class DwsTradePaymentSucWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradePaymentSucWindow();
        driver.execute("DwsTradePaymentSucWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka 支付页面数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_pay_detail_suc.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradePaymentSucWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        // todo 2.转换数据格式为JSONObject

        SingleOutputStreamOperator<JSONObject> mappedDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("不合法数据：" + value);
                }
            }
        });

        // todo 3.按照订单（order_detail_id）分组
        KeyedStream<JSONObject, String> keyedOrderDetailIdStream = mappedDs.keyBy(line -> line.getString("order_detail_id"));

        // todo 4.数据去重（状态编程获取最新的数据）
        SingleOutputStreamOperator<JSONObject> filterDs = keyedOrderDetailIdStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("value-state", JSONObject.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取状态中得数据
                JSONObject lastValue = valueState.value();

                // 判断状态中是否存在数据 不存在则注册一个5s后得定时器
                // 若5s内无数据到达 将该数据作为最终数据进行输出
                if (lastValue == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5 * 1000L);
                } else {
                    // 若存在上条数据 则与当前数据比较时间戳大小 时间戳大的认为该条数据已经更新
                    String currentDt = value.getString("row_op_ts");
                    String stateDt = lastValue.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(stateDt,currentDt);
                    if (compare != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // 获取最新数据输出并清空状态
                JSONObject latestValue = valueState.value();
                out.collect(latestValue);
                valueState.clear();
            }
        });

        // todo 5.提取水位线
        SingleOutputStreamOperator<JSONObject> assignWmDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return DateFormatUtil.toTs(element.getString("callback_time"), true);
            }
        }));

        // todo 6.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = assignWmDs.keyBy(json -> json.getString("user_id"));

        // todo 7.使用状态编程计算支付成功独立用户数和首次支付成功用户
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentWindowDs = keyedByUserIdStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-pay-dt", String.class);
                lastDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                String currentDt = value.getString("callback_time").split(" ")[0];
                String lastDt = lastDtState.value();

                long uvPayCt = 0L;
                long newPayCt = 0L;
                // 判断是否有过上次支付记录
                if (lastDt != null) {
                    // 若上次支付时间不是当前时间则独立支付用户+1
                    if (!currentDt.equals(lastDt)) {
                        uvPayCt = 1L;
                        lastDtState.update(currentDt);
                    }
                } else {
                    uvPayCt = 1L;
                    newPayCt = 1L;
                }

                if (uvPayCt == 1L) {
                    out.collect(new TradePaymentWindowBean("", "", uvPayCt, newPayCt, null));
                }
            }
        });

        // todo 8.开窗 聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDs = tradePaymentWindowDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                @Override
                public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                    value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                    value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                    return value1;
                }
            }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                    TradePaymentWindowBean paymentWindowBean = values.iterator().next();
                    paymentWindowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    paymentWindowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    paymentWindowBean.setTs(System.currentTimeMillis());
                    out.collect(paymentWindowBean);
                }
            });

        // todo 9.将数据写出Clickhouse
        resultDs.print(">>>");
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        ));

    }
}
