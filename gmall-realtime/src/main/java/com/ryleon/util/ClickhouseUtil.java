package com.ryleon.util;

import com.ryleon.bean.TransientSink;
import com.ryleon.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author ALiang
 * @date 2022-12-31
 * @effect Clickhouse工具类
 */
public class ClickhouseUtil {

    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.sink(sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                    Field[] declaredFields = t.getClass().getDeclaredFields();
                    int offset = 0;
                    for (int i = 0; i < declaredFields.length; i++) {
                        Field field = declaredFields[i];
                        // 设置强制访问
                        field.setAccessible(true);
                        // 尝试获取Annotation
                        Annotation annotation = field.getAnnotation(TransientSink.class);
                        if (annotation != null) {
                            offset++;
                            continue;
                        }
                        try {
                            // 获取对象对应的值
                            Object value = field.get(t);
                            // 赋值给preparedStatement
                            preparedStatement.setObject(i + 1 - offset, value);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(5)
                .withBatchIntervalMs(1000L)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(GmallConfig.CLICKHOUSE_URL)
                .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                .build());
    }
}
