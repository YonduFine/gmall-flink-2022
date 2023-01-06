package com.ryleon.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.util.DimUtil;
import com.ryleon.util.PhoenixDSUtil;
import com.ryleon.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimTableSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = PhoenixDSUtil.createDataSource();
    }

    /*
        {"sinkTable":"dim_base_category2","database":"gmall","xid":1350,"data":{"category1_id":1,"name":"test","id":114},"commit":true,"type":"insert","table":"base_category2","ts":1645429751}
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 1.获取连接池
        DruidPooledConnection connection = druidDataSource.getConnection();
        // 2.将数据写入Phoenix表
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        // 3.1若是更新，删除redis中的数据
        String type = value.getString("type");
        String updateStr = "update";
        if (updateStr.equals(type)) {
            DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
        }
        // 3.2写出数据
        PhoenixUtil.upsertValue(connection, sinkTable, data);
        // 4.释放连接
        connection.close();
    }
}
