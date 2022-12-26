package com.ryleon.app.dwd.db;

import com.ryleon.app.base.BaseDwdFlinkApp;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.MySqlUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-26
 * @effect 交易域支付成功事务事实表
 *
 * <p>
 * 数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_pay_detail_suc)
 * <p>
 * 程序：mock->Maxwell->Kafka(ZK)->DwdTradePayDetailSuc->Kafka(ZK)
 */
public class DwdTradePayDetailSuc extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdTradePayDetailSuc();
        driver.execute("DwdTradePayDetailSuc", 905);
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {
        // env.execute(appName);
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤出支付成功信息
        String filterPaySucSql = "SELECT\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['payment_type'] payment_type,\n" +
            "    `data`['callback_time'] callback_time,\n" +
            "    pt\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table` = 'payment_info'\n" +
            "AND `type` = 'update'\n" +
            "AND `data`['payment_status'] = '1602'";
        Table paySucTable = tableEnv.sqlQuery(filterPaySucSql);
        tableEnv.createTemporaryView("payment_info", paySucTable);

        // todo 3.消费kafka 订单主题数据
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.trade_order_detail.topic");
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
            "`row_op_ts` TIMESTAMP_LTZ(3))"
            + MyKafkaUtil.getFlinkKafkaDdl(topic,"dwd_trade_pay_detail_suc");
        tableEnv.executeSql(createResultTableSql);

        // todo 4.创建MySQL lookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // table-name  base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 5.关联表
        String resultSql = "SELECT\n" +
            "    od.id,\n" +
            "    od.order_id,\n" +
            "    od.user_id,\n" +
            "    od.sku_id,\n" +
            "    od.sku_name,\n" +
            "    od.sku_num,\n" +
            "    od.order_price,\n" +
            "    od.province_id,\n" +
            "    od.activity_id,\n" +
            "    od.activity_rule_id,\n" +
            "    od.coupon_id,\n" +
            "    pi.payment_type payment_type_id,\n" +
            "    dic.dic_name payment_type_name,\n" +
            "    pi.callback_time,\n" +
            "    od.create_time,\n" +
            "    od.source_id,\n" +
            "    od.source_type_id,\n" +
            "    od.source_type_name,\n" +
            "    od.split_activity_amount,\n" +
            "    od.split_coupon_amount,\n" +
            "    od.split_total_amount,\n" +
            "    od.row_op_ts\n" +
            "FROM payment_info pi\n" +
            "LEFT JOIN dwd_order_detail od\n" +
            "ON pi.order_id=od.order_id\n" +
            "JOIN base_dic FOR SYSTEM_TIME AS OF pi.pt AS dic\n" +
            "ON pi.payment_type=dic.dic_code";
        Table payDetailSuc = tableEnv.sqlQuery(resultSql);
        tableEnv.createTemporaryView("result_table",payDetailSuc);
        // tableEnv.sqlQuery("select * from result_table").execute().print();

        // todo 6.创建upsert-kafka dwd_trade_pay_detail_suc表
        String targetTopic = properties.getProperty("dwd.kafka.trade_pay_detail_suc.topic");
        String createSql = "CREATE TABLE IF NOT EXISTS dwd_pay_detail_suc(\n" +
            "    id STRING,\n" +
            "    order_id STRING,\n" +
            "    user_id STRING,\n" +
            "    sku_id STRING,\n" +
            "    sku_name STRING,\n" +
            "    sku_num STRING,\n" +
            "    order_price STRING,\n" +
            "    province_id STRING,\n" +
            "    activity_id STRING,\n" +
            "    activity_rule_id STRING,\n" +
            "    coupon_id STRING,\n" +
            "    payment_type_id STRING,\n" +
            "    payment_type_name STRING,\n" +
            "    callback_time STRING,\n" +
            "    create_time STRING,\n" +
            "    source_id STRING,\n" +
            "    source_type_id STRING,\n" +
            "    source_type_name STRING,\n" +
            "    split_activity_amount STRING,\n" +
            "    split_coupon_amount STRING,\n" +
            "    split_total_amount STRING,\n" +
            "    row_op_ts TIMESTAMP_LTZ(3),\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ")"+MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 7.写出数据
        String insertSql = "INSERT INTO dwd_pay_detail_suc SELECT * FROM result_table";
        tableEnv.executeSql(insertSql);
    }
}
