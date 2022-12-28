package com.ryleon.app.dwd.db;

import com.ryleon.app.base.BaseDwdFlinkApp;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-28
 * @effect 工具域优惠券使用（支付）事务事实表
 */
public class DwdToolCouponPay extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdToolCouponPay();
        driver.execute("DwdToolCouponPay");
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤coupon_use表中的新增数据 type='update' used_time is not null
        String filterCouponPaySql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['coupon_id'] coupon_id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['used_time'] used_time,\n" +
            "    ts\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='coupon_use'\n" +
            "AND `type`='update'\n" +
            "AND `data`['used_time'] IS NOT NULL";
        Table filterCouponPayTable = tableEnv.sqlQuery(filterCouponPaySql);
        tableEnv.createTemporaryView("result_table", filterCouponPayTable);

        // todo 3.创建kafka-connector 表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.tool_coupon_pay.topic");
        String createSql = "CREATE TABLE IF NOT EXISTS dwd_coupon_order(\n" +
            "    id STRING,\n" +
            "    coupon_id STRING,\n" +
            "    user_id STRING,\n" +
            "    order_id STRING,\n" +
            "    used_time STRING,\n" +
            "    ts STRING\n" +
            ")\n" + MyKafkaUtil.getFlinkKafkaSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 4.将数据写出
        tableEnv.executeSql("insert into dwd_coupon_order select * from result_table");
    }
}
