package com.ryleon.util;


import java.util.Properties;

/**
 * @author ALiang
 * @date 2022-12-23
 */
public class MySqlUtil {

    private static Properties prop = null;

    static {
        prop = PropertiesUtil.getProperties();
    }

    /**
     * 创建MySQL base_dic LookUp表
     *
     * @return 拼接好的创建语句
     */
    public static String getBaseDicLookUpDdl() {
        return "CREATE TEMPORARY TABLE `base_dic` (\n" +
            "  `dic_code` STRING,\n" +
            "  `dic_name` STRING,\n" +
            "  `parent_code` STRING,\n" +
            "  `create_time` TIMESTAMP,\n" +
            "  `operate_time` TIMESTAMP\n" +
            ")" + getMySqlLookUpTableDdl("base_dic");
    }

    /**
     * 拼接lookUp表的with语句
     *
     * @param tableName Ddl表名
     * @return 拼接好的lookUp表with语句
     */
    public static String getMySqlLookUpTableDdl(String tableName) {
        String mySqlUrl = prop.getProperty("gmall.mysql.hostname");
        String username = prop.getProperty("gmall.mysql.username");
        String password = prop.getProperty("gmall.mysql.password");
        return " WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = '" + mySqlUrl + "',\n" +
            "  'table-name' = '" + tableName + "',\n" +
            "  'username' = '" + username + "',\n" +
            "  'password' = '" + password + "',\n" +
            "  'lookup.cache.max-rows' = '10',\n" + // 设置缓存数据跳数
            "  'lookup.cache.ttl' = '1 hour',\n" + // 设置缓存数据过期时间
            "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
            ")";
    }
}
