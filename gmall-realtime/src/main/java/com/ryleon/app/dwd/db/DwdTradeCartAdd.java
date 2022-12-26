package com.ryleon.app.dwd.db;

import com.ryleon.app.base.BaseDwdFlinkApp;
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
 * @date 2022-12-23
 * @effect 交易域加购事务事实表
 *
 * <p>
 * 数据：web/app->Ngnix->Mysql->Maxwell->Kafka(ODS)->FlinkApp->Kafka(DWD_trade_cart_add)
 * <p>
 * 程序：mock->Maxwell->Kafka(ZK)->DwdTradeCartAdd->Kafka(ZK)
 */
public class DwdTradeCartAdd extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdTradeCartAdd();
        driver.execute("DwdTradeCartAdd", 5);
    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic数据并封装为Flink SQL表
        // String groupId = "dwd_trade_cart_add";
        // String topicDb = MyKafkaUtil.getTopicDb(groupId);
        // tableEnv.executeSql(topicDb);

        // todo 2.读取购物车数据
        Table cartAddTable = tableEnv.sqlQuery("SELECT \n" +
            "    `data`['id'] id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['cart_price'] cart_price,\n" +
            "    `if`(`type`='insert',`data`['sku_num'],CAST(CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT) AS STRING)) sku_num,\n" +
            "    `data`['sku_name'] sku_name,\n" +
            "    `data`['is_checked'] is_checked,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    `data`['operate_time'] operate_time,\n" +
            "    `data`['is_ordered'] is_ordered,\n" +
            "    `data`['order_time'] order_time,\n" +
            "    `data`['source_type'] source_type,\n" +
            "    `data`['source_id'] source_id,\n" +
            "    pt\n" +
            "FROM topic_db\n" +
            "WHERE `database`='gmall' \n" +
            "AND `table`='cart_info'\n" +
            "AND (`type`='insert'\n" +
            "OR (\n" +
            "    `type` = 'update' \n" +
            "    AND `data`['sku_num'] is not null \n" +
            "    AND CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)\n" +
            "))");
        tableEnv.createTemporaryView("cart_info_table", cartAddTable);
        // 打印cart_info数据 test
        tableEnv.toAppendStream(cartAddTable, Row.class).print(">>>>>");

        // todo 3.创建MySQL LookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // base_dic_lookUp_name : base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 4.关联两张表获得明细表
        String associateSql = "SELECT \n" +
            "    ca.id,\n" +
            "    ca.user_id,\n" +
            "    ca.sku_id,\n" +
            "    ca.cart_price,\n" +
            "    ca.sku_num,\n" +
            "    ca.sku_name,\n" +
            "    ca.is_checked,\n" +
            "    ca.create_time,\n" +
            "    ca.operate_time,\n" +
            "    ca.is_ordered,\n" +
            "    ca.order_time,\n" +
            "    ca.source_type source_type_id,\n" +
            "    dic.dic_name source_type_name,\n" +
            "    ca.source_id\n" +
            "FROM cart_info_table ca JOIN base_dic FOR SYSTEM_TIME AS OF ca.pt AS dic\n" +
            "    ON ca.source_type = dic.dic_code";
        Table cartAddAndDicTable = tableEnv.sqlQuery(associateSql);
        tableEnv.createTemporaryView("cart_add_dic_detail", cartAddAndDicTable);

        // todo 5.建立Kafka Connector dwd_trade_cart_add表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.trade_cart_add.topic");
        String createDwdTradeCartAddSql = "create table dwd_cart_add (\n" +
            "id STRING,\n" +
            "user_id STRING,\n" +
            "sku_id STRING,\n" +
            "cart_price STRING,\n" +
            "sku_num STRING,\n" +
            "sku_name STRING,\n" +
            "is_checked STRING,\n" +
            "create_time STRING,\n" +
            "operate_time STRING,\n" +
            "is_ordered STRING,\n" +
            "order_time STRING,\n" +
            "source_type_id STRING,\n" +
            "source_type_name STRING,\n" +
            "source_id STRING\n" +
            ")" + MyKafkaUtil.getFlinkKafkaSinkDdl(targetTopic);
        tableEnv.executeSql(createDwdTradeCartAddSql);

        // todo 6.将关联结果写入Kafka Topic
        // String insertSql = "insert into dwd_cart_add " + cartAddAndDicTable;
        String insertSql = "insert into dwd_cart_add select * from cart_add_dic_detail";
        tableEnv
            .executeSql(insertSql);
        // .print();
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {
        // env.execute(appName);
    }
}
