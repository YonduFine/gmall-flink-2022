package com.ryleon.app.dwd.db;

import com.ryleon.app.base.BaseFlinkApp;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.MySqlUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-24
 * @effect 交易域订单预处理表
 */
public class DwdTradeOrderPreProcess extends BaseFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseFlinkApp driver = new DwdTradeOrderPreProcess();
        driver.execute("DwdTradeOrderPreProcess");
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.读取Kafka ODS层 topic_db数据并创建Flink SQL table 已完成
        // todo 2.过滤出订单明细表数据
        String orderDetailSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['sku_name'] sku_name,\n" +
            "    `data`['order_price'] order_price,\n" +
            "    `data`['sku_num'] sku_num,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    `data`['source_type'] source_type,\n" +
            "    `data`['source_id'] source_id,\n" +
            "    `data`['split_total_amount'] split_total_amount,\n" +
            "    `data`['split_activity_amount'] split_activity_amount,\n" +
            "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
            "    pt\n" +
            "FROM `topic_db`\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_detail'\n" +
            "AND `type`='insert'";
        Table orderDetailTable = tableEnv.sqlQuery(orderDetailSql);
        tableEnv.createTemporaryView("order_detail_info", orderDetailTable);
        // tableEnv.toAppendStream(orderDetailTable, Row.class).print("order_detail>>");

        // todo 3.过滤出订单表数据
        String orderInfoSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['consignee'] consignee,\n" +
            "    `data`['consignee_tel'] consignee_tel,\n" +
            "    `data`['total_amount'] total_amount,\n" +
            "    `data`['order_status'] order_status,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['payment_way'] payment_way,\n" +
            "    `data`['delivery_address'] delivery_address,\n" +
            "    `data`['order_comment'] order_comment,\n" +
            "    `data`['out_trade_no'] out_trade_no,\n" +
            "    `data`['trade_body'] trade_body,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    `data`['operate_time'] operate_time,\n" +
            "    `data`['expire_time'] expire_time,\n" +
            "    `data`['process_status'] process_status,\n" +
            "    `data`['tracking_no'] tracking_no,\n" +
            "    `data`['parent_order_id'] parent_order_id,\n" +
            "    `data`['province_id'] province_id,\n" +
            "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
            "    `data`['coupon_reduce_amount'] coupon_reduce_amount,\n" +
            "    `data`['original_total_amount'] original_total_amount,\n" +
            "    `data`['feight_fee'] feight_fee,\n" +
            "    `data`['feight_fee_reduce'] feight_fee_reduce,\n" +
            "    `data`['refundable_time'] refundable_time, \n" +
            "    `type`," +
            "    `old`" +
            "FROM `topic_db`\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_info'\n" +
            "AND (`type`='insert' OR `type`='update')";
        Table orderInfoTable = tableEnv.sqlQuery(orderInfoSql);
        tableEnv.createTemporaryView("order_info", orderInfoTable);
        // tableEnv.toAppendStream(orderInfoTable, Row.class).print("order_info>>");

        // todo 4.过滤出订单明细活动关联表数据
        String orderActivitySql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['order_detail_id'] order_detail_id,\n" +
            "    `data`['activity_id'] activity_id,\n" +
            "    `data`['activity_rule_id'] activity_rule_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['create_time'] create_time\n" +
            "FROM `topic_db`\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_detail_activity'\n" +
            "AND `type`='insert'";
        Table orderActivityTable = tableEnv.sqlQuery(orderActivitySql);
        tableEnv.createTemporaryView("order_activity_info", orderActivityTable);
        // tableEnv.toAppendStream(orderActivityTable, Row.class).print("order_detail_activity>>");

        // todo 5.过滤出订单明优惠券关联表数据
        String orderCouponSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['order_detail_id'] order_detail_id,\n" +
            "    `data`['coupon_id'] coupon_id,\n" +
            "    `data`['coupon_use_id'] coupon_use_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['create_time'] create_time\n" +
            "FROM `topic_db`\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_detail_coupon'\n" +
            "AND `type`='insert'";
        Table orderCouponTable = tableEnv.sqlQuery(orderCouponSql);
        tableEnv.createTemporaryView("order_coupon_info", orderCouponTable);
        // tableEnv.toAppendStream(orderCouponTable, Row.class).print("order_detail_coupon>>");

        // todo 6.创建MySQL LookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // base_dic_lookUp_name : base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 7.关联五张表获得预处理数据
        String orderPreProcessSql = "SELECT\n" +
            "    od.id,\n" +
            "    od.order_id,\n" +
            "    od.sku_id,\n" +
            "    od.sku_name,\n" +
            "    od.order_price,\n" +
            "    od.sku_num,\n" +
            "    od.create_time,\n" +
            "    od.source_type source_type_id,\n" +
            "    dic.dic_name source_type_name,\n" +
            "    od.source_id,\n" +
            "    od.split_total_amount,\n" +
            "    od.split_activity_amount,\n" +
            "    od.split_coupon_amount,\n" +
            "    oi.consignee,\n" +
            "    oi.consignee_tel,\n" +
            "    oi.total_amount,\n" +
            "    oi.order_status,\n" +
            "    oi.user_id,\n" +
            "    oi.payment_way,\n" +
            "    oi.delivery_address,\n" +
            "    oi.order_comment,\n" +
            "    oi.out_trade_no,\n" +
            "    oi.trade_body,\n" +
            "    oi.operate_time,\n" +
            "    oi.expire_time,\n" +
            "    oi.process_status,\n" +
            "    oi.tracking_no,\n" +
            "    oi.parent_order_id,\n" +
            "    oi.province_id,\n" +
            "    oi.activity_reduce_amount,\n" +
            "    oi.coupon_reduce_amount,\n" +
            "    oi.original_total_amount,\n" +
            "    oi.feight_fee,\n" +
            "    oi.feight_fee_reduce,\n" +
            "    oi.refundable_time,\n" +
            "    oa.id order_detail_activity_id,\n" +
            "    oa.activity_id,\n" +
            "    oa.activity_rule_id,\n" +
            "    oc.id order_detail_coupon_id,\n" +
            "    oc.coupon_id,\n" +
            "    oc.coupon_use_id,\n" +
            "    oi.`type`,\n" +
            "    oi.`old`" +
            "FROM order_detail_info od \n" +
            "LEFT JOIN order_info oi\n" +
            "ON od.order_id = oi.id\n" +
            "LEFT JOIN order_activity_info oa\n" +
            "ON od.id = oa.order_detail_id\n" +
            "LEFT JOIN order_coupon_info oc\n" +
            "ON od.id = oc.order_detail_id\n" +
            "JOIN base_dic FOR SYSTEM_TIME AS OF od.pt as dic\n" +
            "ON od.source_type = dic.dic_code";
        Table orderPreProcessTable = tableEnv.sqlQuery(orderPreProcessSql);
        tableEnv.createTemporaryView("order_pre_process_info", orderPreProcessTable);
        // tableEnv.sqlQuery("select * from order_pre_process_info").execute().print();

        // todo 8.创建dwd_trade_order_pre_process的Kafka Upsert主题表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.trade_order_pre_process.topic");
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
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ") " + MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createOrderPreProcessSql);

        // todo 9.将数据写入dwd_trade_order_pre_process
        String insertSql = "INSERT INTO dwd_trade_order_pre_process SELECT * FROM order_pre_process_info";
        tableEnv
            .executeSql(insertSql)
            .print();
    }
}
