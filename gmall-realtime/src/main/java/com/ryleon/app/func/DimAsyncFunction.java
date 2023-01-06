package com.ryleon.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.util.DimUtil;
import com.ryleon.util.PhoenixDSUtil;
import com.ryleon.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author ALiang
 * @date 2023-01-05
 * @effect DIM异步I/O
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private DruidDataSource dataSource;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction() {

    }

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = PhoenixDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // 创建连接
                    DruidPooledConnection connection = dataSource.getConnection();
                    // 获取主键
                    String key = getKey(input);
                    // 查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);
                    // 关联维度信息
                    join(input, dimInfo);
                    // 关闭连接
                    connection.close();
                    // 将结果写出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败: " + input + ",table" + tableName);
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }
}
