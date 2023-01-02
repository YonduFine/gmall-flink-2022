package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.base.BaseDwsFlinkApp;
import com.ryleon.bean.CartAddUuBean;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.DateFormatUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @date 2023-01-02
 * @effect 交易域加购各窗口汇总表-统计每日各窗口加购独立用户数
 *
 * <p>数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_trade_cart_add)->FlinkApp->Clickhouse
 * <p>程序：mock->Maxwell->Kafka(ZK)->DwdTradeCartAdd->Kafka(ZK)->DwsTradeCartAddUuWindow->Clickhouse
 */
public class DwsTradeCartAddUuWindow extends BaseDwsFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwsFlinkApp driver = new DwsTradeCartAddUuWindow();
        driver.execute("DwsTradeCartAddUuWindow");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka DWD用户加购数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_cart_add.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTradeCartAddUuWindow");
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2.转换数据格式为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(JSON::parseObject);

        // todo 3.分配水位线
        SingleOutputStreamOperator<JSONObject> assignWmDs = jsonObjDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                // 水位线根据加购时间和操作时间进行设置
                String operateTime = element.getString("operate_time");
                // 若不存在操作时间 使用加购时间
                if (operateTime == null) {
                    String createTime = element.getString("create_time");
                    return DateFormatUtil.toTs(createTime, true);
                } else {
                    return DateFormatUtil.toTs(operateTime, true);
                }
            }
        }));

        // todo 4.根据user_id分组
        KeyedStream<JSONObject, String> keyedStream = assignWmDs.keyBy(json -> json.getString("user_id"));

        // todo 5.使用状态编程统计独立用户加购情况
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuDs = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
                ValueStateDescriptor<String> cartDescriptor = new ValueStateDescriptor<>("last-cart-dt", String.class);
                cartDescriptor.enableTimeToLive(ttlConfig);
                lastCartState = getRuntimeContext().getState(cartDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                String lastCartDt = lastCartState.value();
                String currentDt = null;
                String operateTime = value.getString("operate_time");
                if (operateTime == null) {
                    String createTime = value.getString("create_time");
                    currentDt = createTime.split(" ")[0];
                } else {
                    currentDt = operateTime.split(" ")[0];
                }
                long cartCt = !currentDt.equals(lastCartDt) ? 1L : 0L;
                out.collect(new CartAddUuBean("", "", cartCt, DateFormatUtil.toTs(currentDt, false)));
            }
        });

        // todo 6.开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDs = cartAddUuDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
            .reduce(new ReduceFunction<CartAddUuBean>() {
                @Override
                public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                    value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                    return value1;
                }
            }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                    CartAddUuBean addUuBean = values.iterator().next();
                    addUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    addUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    addUuBean.setTs(System.currentTimeMillis());
                    out.collect(addUuBean);
                }
            });

        resultDs.print(">>");
        // todo 7.将结果写入Clickhouse
        resultDs.addSink(ClickhouseUtil.getJdbcSink(
            "insert into dws_trade_cart_add_uu_window values(?,?,?,?)"
        ));
    }
}
