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
 * @effect 互动域收藏商品事务事实表
 */
public class DwdInteractionFavorAdd extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdInteractionFavorAdd();
        driver.execute("DwdInteractionFavorAdd");
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤favor表中的新增数据 type='insert'
        String filterCouponPaySql = "SELECT\n" +
            "    `data`['id'] id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    ts\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall'\n" +
            "AND `table`='favor_info'\n" +
            "AND `type`='insert'";
        Table filterCouponPayTable = tableEnv.sqlQuery(filterCouponPaySql);
        tableEnv.createTemporaryView("result_table", filterCouponPayTable);

        // todo 3.创建kafka-connector 表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.interaction_favor_add.topic");
        String createSql = "CREATE TABLE IF NOT EXISTS dwd_favor_add(\n" +
            "    id STRING,\n" +
            "    user_id STRING,\n" +
            "    sku_id STRING,\n" +
            "    create_time STRING,\n" +
            "    ts STRING\n" +
            ")\n" + MyKafkaUtil.getFlinkKafkaSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 4.将数据写出
        tableEnv.executeSql("insert into dwd_favor_add select * from result_table");
    }
}
