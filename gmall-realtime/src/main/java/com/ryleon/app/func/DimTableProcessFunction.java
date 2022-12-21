package com.ryleon.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.bean.TableProcess;
import com.ryleon.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class DimTableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private final MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public DimTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /*
    {"database":"gmall","table":"base_trademark","type":"insert","ts":1645444616,"xid":519,"commit":true,"data":{"id":12,"tm_name":"Honor","logo_url":null}}
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1.获取广播流数据
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState =
            ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = value.getString("table");
        TableProcess tableProcess = tableConfigState.get(sourceTable);

        if (tableProcess != null) {
            // 2.过滤数据
            JSONObject data = value.getJSONObject("data");
            filterColumn(data, tableProcess.getSinkColumns());
            // 3.补充sinkTable字段后写出
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
        } else {
            System.out.println(">>>>> 维表不存在：" + sourceTable);
        }

    }

    /**
     * 根据SinkColumns过滤出主流数据
     *
     * @param data        原始列信息
     * @param sinkColumns 维表需要的列信息
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] sinkColumnArr = sinkColumns.split(",");
        List<String> sinkColumnList = Arrays.asList(sinkColumnArr);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            String column = iterator.next().getKey();
            if (!sinkColumnList.contains(column)) {
                iterator.remove();
            }
        }
    }

    /*
    {"before":null,"after":{"source_table":"aa","sink_table":"aa","sink_columns":"aa","sink_pk":"aa","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1671354672150,"snapshot":"false","db":"gmall_config_2212","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1671354672156,"transaction":null}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.获取广播流数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        // 2.检查Phoenix中表是否存在
        checkTable(
            tableProcess.getSinkTable(),
            tableProcess.getSinkColumns(),
            tableProcess.getSinkPk(),
            tableProcess.getSinkExtend());

        // 3.将配置表数据写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);
    }

    /**
     * 检查对应的维度表在Phoenix是否创建
     * create table if not exists db.table(aa varchar primary key, bb varchar) xxx;
     *
     * @param sinkTable   输出表名
     * @param sinkColumns 输出表字段
     * @param sinkPk      表主键信息
     * @param sinkExtend  表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        if (sinkPk == null) {
            sinkPk = "id";
        }
        PreparedStatement preparedStatement = null;

        try {
            StringBuilder createDimTable = new StringBuilder()
                .append("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

            String[] columnArr = sinkColumns.split(",");
            for (int i = 0; i < columnArr.length; i++) {
                if (sinkPk.equals(columnArr[i])) {
                    createDimTable.append(columnArr[i]).append(" varchar primary key");
                } else {
                    createDimTable.append(columnArr[i]).append(" varchar");
                }
                if (i < columnArr.length - 1) {
                    createDimTable.append(",");
                }
            }
            createDimTable.append(")")
                .append(" ")
                .append(sinkExtend);
            String createDimSql = createDimTable.toString();
            System.out.println("在Phoenix建表语句为：" + createDimSql);
            preparedStatement = connection.prepareStatement(createDimSql);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("[ERROR] " + sinkTable + "表不存在");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
