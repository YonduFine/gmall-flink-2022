package com.ryleon.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ryleon.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author ALiang
 * @date 2023-01-04
 * @effect DIM层Hbase数据库操作
 */
public class DimUtil {

    /**
     * <p>根据表与id查询指定的数据</p>
     *
     * @param connection Phoenix连接对象
     * @param tableName  表名
     * @param key        id
     * @return JSONObject
     * @throws Exception 向调用者返回异常信息
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws Exception {
        // 先查询Redis中数据是否存在
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + GmallConfig.HBASE_SCHEMA + ":" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if (dimJsonStr != null) {
            // 重置过期时间
            jedis.expire(redisKey, DateFormatUtil.days(1));
            // 归还连接
            jedis.close();
            // 返回数据
            return JSON.parseObject(dimJsonStr);
        }
        // 若数据在Redis中不存在查询Hbase
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'";
        List<JSONObject> resultList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = resultList.get(0);
        // 将数据放入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        // 设置过期时间
        jedis.expire(redisKey, DateFormatUtil.days(1));
        // 归还连接
        jedis.close();
        // 返回数据
        return dimInfoJson;
    }

    public static void delDimInfo(String tableName,String key){
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + GmallConfig.HBASE_SCHEMA + ":" + tableName + ":" + key;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = PhoenixDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end = System.currentTimeMillis();
        JSONObject dimInfo1 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end1 = System.currentTimeMillis();
        System.out.println(dimInfo);
        // 291 183 213 146 167 165 137
        System.out.println(end - start);
        // 11 11 11 3 2 1 2
        System.out.println(end1 - end);


    }
}
