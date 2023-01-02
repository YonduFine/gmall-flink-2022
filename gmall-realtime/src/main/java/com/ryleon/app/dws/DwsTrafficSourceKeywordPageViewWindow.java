package com.ryleon.app.dws;

import cn.hutool.core.util.StrUtil;
import com.ryleon.app.func.DwsSplitFunction;
import com.ryleon.bean.KeywordBean;
import com.ryleon.common.GmallConstant;
import com.ryleon.util.ClickhouseUtil;
import com.ryleon.util.MyKafkaUtil;
import com.ryleon.util.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-28
 * @effect 流量域来源关键词粒度页面浏览各窗口汇总表（FlinkSQL）
 * <p>
 * 数据：web/app->Ngnix->日志服务器->.log->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Kafka(DWD_pageLog)->FlinkApp->ClickHouse
 * <p>
 * 程序：mock(lg.sh)->f1_log.sh(Flume)->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUniqueVisitorDetail->Kafka(ZK)->DwsTrafficSourceKeywordPageViewWindow -> Clickhouse
 */
public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {
        // todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 创建Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        // 1.2 设置检查点
        // env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        // 1.3 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata101:9820/FlinkAppCk");
        // 1.4 设置用户信息
        // System.setProperty("HADOOP_USER_NAME","ryl");

        // todo 2.消费dwd_traffic_page_log数据并添加watermark
        Properties properties = PropertiesUtil.getProperties();
        String topic = properties.getProperty("dwd.kafka.traffic_page_log.topic");
        String groupId = StrUtil.toUnderlineCase("DwsTrafficSourceKeywordPageViewWindow");
        String pageLogSql = "create table page_log(\n" +
            "    `page` MAP<STRING,STRING>,\n" +
            "    `ts` BIGINT,\n" +
            "    `rt` AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
            "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
            ")" + MyKafkaUtil.getFlinkKafkaDdl(topic, groupId);
        tableEnv.executeSql(pageLogSql);

        // todo 3.过滤出搜索数据
        String filterSql = "select\n" +
            "   page['item'] item,\n" +
            "   rt\n" +
            "from page_log\n" +
            "where page['item_type'] = 'keyword'\n" +
            "and page['last_page_id'] = 'search'\n" +
            "and page['item'] is not null";
        Table filterTable = tableEnv.sqlQuery(filterSql);
        tableEnv.createTemporaryView("filter_table", filterTable);

        // todo 4.创建&注册自定义函数
        tableEnv.createTemporarySystemFunction("SplitFunction", DwsSplitFunction.class);

        // todo 5.使用分词函数对关键词进行分词
        String splitWordSql = "SELECT\n" +
            "    item,\n" +
            "    word,\n" +
            "    rt\n" +
            "FROM filter_table, LATERAL TABLE(SplitFunction(item))";
        Table splitWordTable = tableEnv.sqlQuery(splitWordSql);
        tableEnv.createTemporaryView("split_word", splitWordTable);

        // todo 6.开窗统计关键词
        String aggregateSql = "SELECT\n" +
            "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
            "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
            "    '" + GmallConstant.KEYWORD_SEARCH + "' source,\n" +
            "    word keyword,\n" +
            "    count(*) keyword_count,\n" +
            "    UNIX_TIMESTAMP()*1000 ts\n" +
            "FROM split_word\n" +
            "GROUP BY word,TUMBLE(rt, INTERVAL '10' SECOND)";
        Table aggregateTable = tableEnv.sqlQuery(aggregateSql);

        // todo 7.将结果由Flink table转换为DataStream
        DataStream<KeywordBean> keywordDataStream = tableEnv.toAppendStream(aggregateTable, KeywordBean.class);
        keywordDataStream.print(">>");

        // todo 8.将结果写入Clickhouse
        keywordDataStream.addSink(ClickhouseUtil.getJdbcSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        // todo 9.启动程序
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
