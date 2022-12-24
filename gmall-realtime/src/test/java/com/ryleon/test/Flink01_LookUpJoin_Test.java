package com.ryleon.test;

import com.ryleon.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ALiang
 * @date 2022-12-23
 */
public class Flink01_LookUpJoin_Test {
    public static void main(String[] args) {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.获取Flink SQL执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建表
        // 要求：表字段必须与数据库表字段一致 不要求创建表时获取全部字段
        tableEnv.executeSql("CREATE TEMPORARY TABLE base_dic (\n" +
            "  dic_code STRING,\n" +
            "  dic_name STRING,\n" +
            "  parent_code STRING,\n" +
            "  create_time STRING,\n" +
            "  operate_time STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:mysql://bigdata101:3306/gmall',\n" +
            "  'table-name' = 'base_dic',\n" +
            "  'username' = 'root',\n" +
            "  'password' = '123456',\n" +
            "  'lookup.cache.max-rows' = '10',\n" + // 设置缓存数据跳数
            "  'lookup.cache.ttl' = '1 hour',\n" + // 设置缓存数据过期时间
            "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
            ")");

        // 查询并打印表
        // tableEnv.sqlQuery("select dic_code,dic_name from base_dic")
        //     .execute()
        //     .print();

        // 构建事实表
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("localhost", 9999)
            .map(line -> {
                String[] split = line.split(",");
                return new WaterSensor(split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
            });

        Table table = tableEnv.fromDataStream(waterSensorDs, $("id"), $("vc"), $("ts"), $("pt").proctime());
        tableEnv.createTemporaryView("t1",table);

        tableEnv.sqlQuery("SELECT t1.id,dic.dic_name,t1.pt\n" +
                "FROM t1 \n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF t1.pt AS dic\n" +
                "    ON t1.id = dic.dic_code")
            .execute().print();
    }
}
