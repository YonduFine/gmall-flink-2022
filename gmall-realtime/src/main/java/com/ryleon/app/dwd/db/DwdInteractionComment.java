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
 * @date 2022-12-28
 * @effect 互动域评价事务事实表
 */
public class DwdInteractionComment extends BaseDwdFlinkApp {

    public static void main(String[] args) throws Exception {
        BaseDwdFlinkApp driver = new DwdInteractionComment();
        driver.execute("DwdInteractionComment", 5);
    }

    @Override
    public void startEnv(StreamExecutionEnvironment env, String appName) throws Exception {

    }

    @Override
    public void process(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws Exception {
        // todo 1.消费Kafka topic_db数据并创建flink-sql-table topic_db
        // todo 2.过滤comment_info表
        String filterCommentInfoSql = "select\n" +
            "    `data`['id'] id,\n" +
            "    `data`['user_id'] user_id,\n" +
            "    `data`['sku_id'] sku_id,\n" +
            "    `data`['spu_id'] spu_id,\n" +
            "    `data`['order_id'] order_id,\n" +
            "    `data`['appraise'] appraise,\n" +
            "    `data`['create_time'] create_time,\n" +
            "    pt,\n" +
            "    ts\n" +
            "from topic_db\n" +
            "where `database`='gmall'\n" +
            "and `table`='comment_info'\n" +
            "and `type`='insert'";
        Table filterCommentInfoTable = tableEnv.sqlQuery(filterCommentInfoSql);
        tableEnv.createTemporaryView("comment_info", filterCommentInfoTable);

        // todo 3.创建MySQL lookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        // table-name base_dic
        tableEnv.executeSql(baseDicLookUpDdl);

        // todo 4.关联表
        String commentJoinDicSql = "select\n" +
            "    ci.id,\n" +
            "    ci.user_id,\n" +
            "    ci.sku_id,\n" +
            "    ci.spu_id,\n" +
            "    ci.order_id,\n" +
            "    ci.appraise appraise_type_id,\n" +
            "    dic.dic_name appraise_type_name,\n" +
            "    ci.create_time,\n" +
            "    ci.ts\n" +
            "from comment_info ci\n" +
            "JOIN base_dic FOR SYSTEM_TIME AS OF ci.pt AS dic\n" +
            "ON ci.appraise = dic.dic_code";
        Table commentJoinDicTable = tableEnv.sqlQuery(commentJoinDicSql);
        tableEnv.createTemporaryView("result_table", commentJoinDicTable);

        // todo 5.创建upsert-kafka 表
        Properties properties = PropertiesUtil.getProperties();
        String targetTopic = properties.getProperty("dwd.kafka.interaction_comment.topic");
        String createSql = "create table if not exists dwd_comment(\n" +
            "    id STRING,\n" +
            "    user_id STRING,\n" +
            "    sku_id STRING,\n" +
            "    spu_id STRING,\n" +
            "    order_id STRING,\n" +
            "    appraise_type_id STRING,\n" +
            "    appraise_type_name STRING,\n" +
            "    create_time STRING,\n" +
            "    ts STRING,\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ")" + MyKafkaUtil.getFlinkKafkaUpsertSinkDdl(targetTopic);
        tableEnv.executeSql(createSql);

        // todo 6.将数据写出
        tableEnv.executeSql("insert into dwd_comment select * from result_table");
    }
}
