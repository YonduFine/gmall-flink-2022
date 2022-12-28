package com.ryleon.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author Lenovo
 */
public class MyKafkaUtil {
    private static String KAFKA_BOOTSTRAP_SERVER = "";
    private static Properties prop = null;

    static {
        prop = PropertiesUtil.getProperties();
        KAFKA_BOOTSTRAP_SERVER = prop.getProperty("Kafka.bootstrap_server.url");
    }

    /**
     * Kafka-Upsert Ddl 语句
     *
     * @param topic 要写入数据的kafka topic
     * @return 拼接好的Kafka Upsert Sink with语句
     */
    public static String getFlinkKafkaUpsertSinkDdl(String topic) {
        return " WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = '" + topic + "',\n" +
            "  'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP_SERVER + "',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")";
    }

    /**
     * topic_db主题的Kafka-Source DDL 语句
     *
     * @param groupId 消费者组
     * @return create table sql
     */
    public static String getTopicDb(String groupId) {
        String topic = prop.getProperty("ods.kafka.db.topic.name");
        return "CREATE TABLE IF NOT EXISTS topic_db (\n" +
            "  `database` STRING,\n" +
            "  `table` STRING,\n" +
            "  `type` STRING,\n" +
            "  `data` MAP<STRING,STRING>,\n" +
            "  `old` MAP<STRING,STRING>,\n" +
            "  `ts` STRING,\n" +
            "  pt AS PROCTIME()\n" +
            ")" + getFlinkKafkaDdl(topic, groupId);
    }

    /**
     * 获取FlinkKafka 的with配置
     *
     * @param topic   Kafka主题
     * @param groupId 消费者组
     * @return flinkKafkaSql with部分配置
     */
    public static String getFlinkKafkaDdl(String topic, String groupId) {
        return "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '" + topic + "',\n" +
            "  'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP_SERVER + "',\n" +
            "  'properties.group.id' = '" + groupId + "',\n" +
            "  'scan.startup.mode' = 'group-offsets',\n" +
            "  'format' = 'json'\n" +
            ")";
    }

    /**
     * 获取FlinkKafkaSink 的with配置
     *
     * @param topic topic输出到Kafka目标主题
     * @return flinkKafkaSql with部分配置
     */
    public static String getFlinkKafkaSinkDdl(String topic) {
        return "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '" + topic + "',\n" +
            "  'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP_SERVER + "',\n" +
            "  'format' = 'json'\n" +
            ")";
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        return new FlinkKafkaProducer<String>(
            topic,
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                    return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                }
            },
            properties,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(
            topic,
            new KafkaDeserializationSchema<String>() {
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }

                @Override
                public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                    if (record == null || record.value() == null) {
                        return "";
                    } else {
                        return new String(record.value());
                    }
                }

                @Override
                public TypeInformation<String> getProducedType() {
                    return BasicTypeInfo.STRING_TYPE_INFO;
                }
            },
            properties
        );
    }

    public static void main(String[] args) {
        System.out.println(KAFKA_BOOTSTRAP_SERVER);
    }
}
