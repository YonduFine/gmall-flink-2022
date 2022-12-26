package com.ryleon.app.dwd.db;

import cn.hutool.core.util.StrUtil;
import com.ryleon.app.base.BaseDwdFlinkApp;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-25
 * @effect 交易域下单事务事实表
 * <p>
 * 数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_pre_Process)->FlinkApp->Kafka(dwd_trade_order_detail)
 * <p>
 * 程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderPreProcess->Kafka(ZK)->DwdTradeOrderDetail->Kafka(ZK)
 */
public class DwdTradeOrderDetail extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdTradeOrderDetail();
        driver.execute("DwdTradeOrderDetail");
    }

    @Override
    public void loadData(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String appName) throws Exception {
        // todo 1.从dwdTradeOrderPreProcess Kafka-Topic中读取数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_pre_process.topic");
        String groupId = StrUtil.toUnderlineCase(appName);
        String createOrderPreProcessSql = "CREATE TABLE IF NOT EXISTS dwd_trade_order_pre_process(\n" +
            "    `id` STRING,\n" +
            "    `order_id` STRING,\n" +
            "    `sku_id` STRING,\n" +
            "    `sku_name` STRING,\n" +
            "    `order_price` STRING,\n" +
            "    `sku_num` STRING,\n" +
            "    `create_time` STRING,\n" +
            "    `source_type_id` STRING,\n" +
            "    `source_type_name` STRING,\n" +
            "    `source_id` STRING,\n" +
            "    `split_total_amount` STRING,\n" +
            "    `split_activity_amount` STRING,\n" +
            "    `split_coupon_amount` STRING,\n" +
            "    `consignee` STRING,\n" +
            "    `consignee_tel` STRING,\n" +
            "    `total_amount` STRING,\n" +
            "    `order_status` STRING,\n" +
            "    `user_id` STRING,\n" +
            "    `payment_way` STRING,\n" +
            "    `delivery_address` STRING,\n" +
            "    `order_comment` STRING,\n" +
            "    `out_trade_no` STRING,\n" +
            "    `trade_body` STRING,\n" +
            "    `operate_time` STRING,\n" +
            "    `expire_time` STRING,\n" +
            "    `process_status` STRING,\n" +
            "    `tracking_no` STRING,\n" +
            "    `parent_order_id` STRING,\n" +
            "    `province_id` STRING,\n" +
            "    `activity_reduce_amount` STRING,\n" +
            "    `coupon_reduce_amount` STRING,\n" +
            "    `original_total_amount` STRING,\n" +
            "    `feight_fee` STRING,\n" +
            "    `feight_fee_reduce` STRING,\n" +
            "    `refundable_time` STRING,\n" +
            "    `order_detail_activity_id` STRING,\n" +
            "    `activity_id` STRING,\n" +
            "    `activity_rule_id` STRING,\n" +
            "    `order_detail_coupon_id` STRING,\n" +
            "    `coupon_id` STRING,\n" +
            "    `coupon_use_id` STRING,\n" +
            "    `type` STRING,\n" +
            "    `old` MAP<STRING,STRING>, \n" +
            "    row_op_ts timestamp_ltz(3)\n" +
            ") " + MyKafkaUtil.getFlinkKafkaDdl(topic, groupId);
        tableEnv.executeSql(createOrderPreProcessSql);
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 2.过滤出下单数据
        String filterOrderSql = "SELECT " +
            "id,\n" +
            "order_id,\n" +
            "user_id,\n" +
            "sku_id,\n" +
            "sku_name,\n" +
            "sku_num,\n" +
            "order_price,\n" +
            "province_id,\n" +
            "activity_id,\n" +
            "activity_rule_id,\n" +
            "coupon_id,\n" +
            // "date_id,\n" +
            "create_time,\n" +
            "source_id,\n" +
            "source_type_id,\n" +
            "source_type_name,\n" +
            // "sku_num,\n" +
            // "split_original_amount,\n" +
            "split_activity_amount,\n" +
            "split_coupon_amount,\n" +
            "split_total_amount,\n" +
            // "od_ts ts,\n" +
            "row_op_ts\n" +
            "FROM dwd_trade_order_pre_process " +
            "WHERE `type`='insert'";
        Table filterOrderTable = tableEnv.sqlQuery(filterOrderSql);
        tableEnv.createTemporaryView("result_table", filterOrderTable);

        // todo 3.创建dwd_trade_order_detail Upsert-Kafka表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.trade_order_detail.topic");
        String createResultTableSql = "CREATE TABLE IF NOT EXISTS dwd_order_detail(" +
            "`id` STRING,\n" +
            "`order_id` STRING,\n" +
            "`user_id` STRING,\n" +
            "`sku_id` STRING,\n" +
            "`sku_name` STRING,\n" +
            "`sku_num` STRING,\n" +
            "`order_price` STRING,\n" +
            "`province_id` STRING,\n" +
            "`activity_id` STRING,\n" +
            "`activity_rule_id` STRING,\n" +
            "`coupon_id` STRING,\n" +
            "`create_time` STRING,\n" +
            "`source_id` STRING,\n" +
            "`source_type_id` STRING,\n" +
            "`source_type_name` STRING,\n" +
            "`split_activity_amount` STRING,\n" +
            "`split_coupon_amount` STRING,\n" +
            "`split_total_amount` STRING,\n" +
            "`row_op_ts` TIMESTAMP_LTZ(3)," +
            "PRIMARY KEY (id) NOT ENFORCED )"
            + MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createResultTableSql);

        // todo 4.将数据写出
        String insertSql = "insert into dwd_order_detail select * from result_table";
        tableEnv.executeSql(insertSql);
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {
        // env.execute(appName);
    }
}
