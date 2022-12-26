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
 * @effect 交易域退单事务事实表
 *
 * <p>
 * 数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_order_refund)
 * <p>
 * 程序：mock->Maxwell->Kafka(ZK)->DwdTradeOrderRefund->Kafka(ZK)
 */
public class DwdTradeOrderRefund extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdTradeOrderRefund();
        driver.execute("DwdTradeOrderRefund", 5);
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤退单事务表数据
        String filterOrderRefundSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['refund_type'] refund_type,\n" +
            "    `data`['refund_num'] refund_num,\n" +
            "    `data`['refund_amount'] refund_amount,\n" +
            "    `data`['refund_reason_type'] refund_reason_type,\n" +
            "    `data`['refund_reason_txt'] refund_reason_txt,\n" +
            "    `data`['refund_status'] refund_status,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    pt\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_refund_info'\n" +
            "AND `type`='insert'";
        Table filterOrderRefundTable = tableEnv.sqlQuery(filterOrderRefundSql);
        tableEnv.createTemporaryView("order_refund_info", filterOrderRefundTable);

        // todo 3.过滤order_info表数据
        String filterOrderSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['province_id'] province_id,\n" +
            "    `old`\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='order_info'\n" +
            "AND `data`['order_status'] = '1005'\n" +
            "AND `type`='update'\n" +
            "AND `old`['order_status'] IS NOT NULL\n";
        Table filterOrderTable = tableEnv.sqlQuery(filterOrderSql);
        tableEnv.createTemporaryView("order_info", filterOrderTable);

        // todo 4.创建MySQL lookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // table-name  base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 5.关联表
        String resultSql = "SELECT\n" +
            "    ori.id,\n" +
            "    ori.user_id,\n" +
            "    ori.order_id,\n" +
            "    ori.sku_id,\n" +
            "    oi.province_id,\n" +
            "    ori.refund_type refund_type_id,\n" +
            "    type_dic.dic_name refund_type_name,\n" +
            "    ori.refund_num,\n" +
            "    ori.refund_amount,\n" +
            "    ori.refund_reason_type refund_reason_type_id,\n" +
            "    dic.dic_name refund_reason_type_name,\n" +
            "    ori.refund_reason_txt,\n" +
            "    ori.refund_status,\n" +
            "    ori.create_time,\n" +
            "    current_row_timestamp() row_op_ts\n" +
            "FROM order_refund_info ori\n" +
            "LEFT JOIN order_info oi \n" +
            "ON ori.order_id = oi.id\n" +
            "JOIN base_dic FOR SYSTEM_TIME AS OF ori.pt AS dic\n" +
            "ON ori.refund_reason_type = dic.dic_code\n" +
            "JOIN base_dic FOR SYSTEM_TIME AS OF ori.pt AS type_dic\n" +
            "ON ori.refund_type = type_dic.dic_code";
        Table resultTable = tableEnv.sqlQuery(resultSql);
        tableEnv.createTemporaryView("result_table", resultTable);

        // todo 6.创建upsert-kafka dwd_trade_order_refund 表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.trade_order_refund.topic");
        String createSql = "create table dwd_order_refund(\n" +
            "    id STRING,\n" +
            "    user_id STRING,\n" +
            "    order_id STRING,\n" +
            "    sku_id STRING,\n" +
            "    province_id STRING,\n" +
            "    refund_type_id STRING,\n" +
            "    refund_type_name STRING,\n" +
            "    refund_num STRING,\n" +
            "    refund_amount STRING,\n" +
            "    refund_reason_type_id STRING,\n" +
            "    refund_reason_type_name STRING,\n" +
            "    refund_reason_txt STRING,\n" +
            "    refund_status STRING,\n" +
            "    create_time STRING,\n" +
            "    row_op_ts TIMESTAMP_LTZ(3),\n" +
            "    PRIMARY KEY (id) NOT ENFORCED)" + MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 7.写出数据
        String insertSql = "INSERT INTO dwd_order_refund SELECT * FROM result_table";
        tableEnv.executeSql(insertSql);
    }
}
