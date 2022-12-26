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
 * @effect 交易域退款成功事务事实表
 *
 * <p>
 * 数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_refund_pay_suc)
 * <p>
 * 程序：mock->Maxwell->Kafka(ZK)->DwdTradeRefundPaySuc->Kafka(ZK)
 */
public class DwdTradeRefundPaySuc extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdTradeRefundPaySuc();
        driver.execute("DwdTradeRefundPaySuc ", 5);
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤退单表退款成功表数据 refund_status = 0705
        String filterRefundSucSql = "SELECT\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['refund_num'] refund_num\n" +
            "FROM topic_db\n" +
            "WHERE `database` = 'gmall'\n" +
            "AND `table` = 'order_refund_info'\n" +
            "AND `type` = 'update'\n" +
            "AND `data`['refund_status'] = '0705'\n" +
            "AND `old`['refund_status'] IS NOT NULL";
        Table filterRefundSucTable = tableEnv.sqlQuery(filterRefundSucSql);
        tableEnv.createTemporaryView("refund_suc_info", filterRefundSucTable);

        // todo 3.过滤订单表订单状态为1006订单数据
        String filterOrderRefundSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['province_id'] province_id,\n" +
            "    `old`\n" +
            "FROM topic_db\n" +
            "WHERE `database` = 'gmall'\n" +
            "AND `table` = 'order_info'\n" +
            "AND `type` = 'update'\n" +
            "AND `data`['order_status'] = '1006'\n" +
            "AND `old`['order_status'] IS NOT NULL";
        Table filterOrderRefundTable = tableEnv.sqlQuery(filterOrderRefundSql);
        tableEnv.createTemporaryView("order_refund_info", filterOrderRefundTable);

        // todo 4.过滤退款表退款成功树 refund_status = 0705 (这里由于mock生成数据原因，故状态为0701即为成功)
        String filterPaymentSucSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['payment_type'] payment_type,\n" +
            "    `data`['total_amount'] total_amount,\n" +
            "    `data`['callback_time'] callback_time,\n" +
            "    pt\n" +
            "FROM topic_db\n" +
            "WHERE `database` = 'gmall'\n" +
            "AND `table` = 'refund_payment'\n" //+
            // "AND `type` = 'update'\n" +
            // "AND `data`['order_status'] = '0705'\n" +
            // "AND `old`['order_status'] IS NOT NULL"
            ;
        Table filterPaymentSucTable = tableEnv.sqlQuery(filterPaymentSucSql);
        tableEnv.createTemporaryView("refund_payment_info", filterPaymentSucTable);

        // todo 5.创建MySQL LookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // table-name base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 6.关联四张表数据
        String resultSql = "\n" +
            "select\n" +
            "    rp.id,\n" +
            "    oi.user_id,\n" +
            "    rp.order_id,\n" +
            "    rp.sku_id,\n" +
            "    oi.province_id,\n" +
            "    rp.payment_type payment_type_id,\n" +
            "    dic.dic_name payment_type_name,\n" +
            "    rs.refund_num,\n" +
            "    rp.total_amount,\n" +
            "    rp.callback_time,\n" +
            "    current_row_timestamp() row_op_ts\n" +
            "from refund_payment_info rp\n" +
            "join refund_suc_info rs\n" +
            "on rp.order_id = rs.order_id\n" +
            "join order_refund_info oi\n" +
            "on rp.order_id = oi.id\n" +
            "join base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic\n" +
            "on rp.payment_type = dic.dic_code";
        Table resultTable = tableEnv.sqlQuery(resultSql);
        tableEnv.createTemporaryView("result_table", resultTable);

        // todo 7.创建upsert-kafka 事务表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.trade_refund_pay_suc.topic");
        String createSql = "create table if not exists dwd_refund_pay_suc(\n" +
            "    id STRING,\n" +
            "    user_id STRING,\n" +
            "    order_id STRING,\n" +
            "    sku_id STRING,\n" +
            "    province_id STRING,\n" +
            "    payment_type_id STRING,\n" +
            "    payment_type_name STRING,\n" +
            "    refund_num STRING,\n" +
            "    total_amount STRING,\n" +
            "    callback_time STRING,\n" +
            "    row_op_ts TIMESTAMP_LTZ(3),\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ")"+ MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 8.将数据写出
        String insertSql = "INSERT INTO dwd_refund_pay_suc SELECT * FROM result_table";
        tableEnv.executeSql(insertSql);
    }
}
