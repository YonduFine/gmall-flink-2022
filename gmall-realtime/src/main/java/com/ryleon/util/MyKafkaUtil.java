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

public class MyKafkaUtil {

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        Properties prop = PropertiesUtil.getProperties();
        String KAFKA_BOOTSTRAP_SERVER = prop.getProperty("Kafka.bootstrap_server.url");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BOOTSTRAP_SERVER);
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
        Properties prop = PropertiesUtil.getProperties();
        String KAFKA_BOOTSTRAP_SERVER = prop.getProperty("Kafka.bootstrap_server.url");

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
}
