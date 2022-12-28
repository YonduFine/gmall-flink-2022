package com.ryleon.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.app.func.DimTableProcessFunction;
import com.ryleon.app.func.DimTableSinkFunction;
import com.ryleon.bean.TableProcess;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

// 数据流：web/app->ngnix->mysql(binlog)->maxwell->kafka(ods)->FlinkApp->Phoenix->Hbase
// 程序流：mock->mysql->maxwell->kafka(zk)->DimApp(Flink CDC/mysql)->Phoenix(Hbase/zk/HDFS)
public class DimApp {
    public static void main(String[] args) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 设置检查点
        // env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        // 1.2 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.3 设置用户
        // System.setProperty("HADOOP_USER_NAME", "ryl");

        // todo 2.读取Kafka topic_db数据，创建主流
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("ods.kafka.db.topic.name");
        String groupId = properties.getProperty("dim.kafka.group_id.name");
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3.过滤掉非JSON数据&保留新增、变化以及初始化数据
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(value);
                    String type = jsonObj.getString("type");
                    List<String> saveList = Arrays.asList("insert", "update", "bootstrap-insert");
                    if (saveList.contains(type)) {
                        out.collect(jsonObj);
                    }
                } catch (Exception e) {
                    System.err.println("格式非法数据：" + value);
                }
            }
        });

        // todo 4.使用Flink CDC读取MySQL配置表信息创建配置流
        String hostname = properties.getProperty("flink_cdc.database.hostname");
        int port = Integer.parseInt(properties.getProperty("flink_cdc.database.port"));
        String username = properties.getProperty("flink_cdc.database.username");
        String password = properties.getProperty("flink_cdc.database.password");
        String dbList = properties.getProperty("flink_cdc.database.list");
        String tableList = properties.getProperty("flink_cdc.database.tableList");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .username(username)
            .password(password)
            .deserializer(new JsonDebeziumDeserializationSchema())
            .startupOptions(StartupOptions.initial())
            .databaseList(dbList)
            .tableList(tableList)
            .build();
        DataStreamSource<String> mySqlSourceDS = env.
            fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // todo 5.将配置流转换为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
            new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mySqlSourceDS.broadcast(mapStateDescriptor);
        // todo 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterJsonObjDS.connect(broadcast);
        // todo 7.处理主流数据，根据配置流数据数据主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new DimTableProcessFunction(mapStateDescriptor));
        dimDS.print();

        // todo 8.将处理后的数据写入Phoenix（Hbase）中
        dimDS.addSink(new DimTableSinkFunction());
        // todo 9.启动任务
        env.execute("DimApp");
    }
}
