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
 * @effect 工具域优惠券使用（下单）事务事实表
 */
public class DwdToolCouponOrder extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdToolCouponOrder();
        driver.execute("DwdToolCouponOrder");
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤coupon_use表中的新增数据 type='update' coupon_status = 1402
        String filterCouponOrderSql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['coupon_id'] coupon_id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['using_time'] using_time,\n" +
            "    ts\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='coupon_use'\n" +
            "AND `type`='update'\n" +
            "AND `data`['coupon_status']='1402'\n" +
            "AND `old`['coupon_status']='1401'";
        Table filterCouponOrderTable = tableEnv.sqlQuery(filterCouponOrderSql);
        tableEnv.createTemporaryView("result_table", filterCouponOrderTable);

        // todo 3.创建kafka-connector 表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.tool_coupon_order.topic");
        String createSql = "CREATE TABLE IF NOT EXISTS dwd_coupon_order(\n" +
            "    id STRING,\n" +
            "    coupon_id STRING,\n" +
            "    user_id STRING,\n" +
            "    order_id STRING,\n" +
            "    using_time STRING,\n" +
            "    ts STRING\n" +
            ")\n" + MyKafkaUtil.getFlinkKafkaSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 4.将数据写出
        tableEnv.executeSql("insert into dwd_coupon_order select * from result_table");

    }
}
