package com.ryleon.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    /**
     * 将数据写入Phoenix表
     * upsert into db.table(id,name) values("xx","xx");
     *
     * @param connection 连接
     * @param sinkTable  写入表名
     * @param data       写入数据
     */
    public static void upsertValue(DruidPooledConnection connection, String sinkTable, JSONObject data){
        // 1.拼接插入语句
        StringBuilder upsertSQL = new StringBuilder()
            .append("upsert into ")
            .append(GmallConfig.HBASE_SCHEMA)
            .append(".")
            .append(sinkTable)
            .append("(");
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        upsertSQL
            .append(StringUtils.join(columns, ","))
            .append(") values ('")
            .append(StringUtils.join(values, "','"))
            .append("')");
        System.out.println("### Dim Insert SQL: " + upsertSQL);

        // 2.执行SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(upsertSQL.toString());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表:" + sinkTable + ",数据[" + data + "]写入异常");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException("资源关闭异常");
                }
            }
        }

    }
}
