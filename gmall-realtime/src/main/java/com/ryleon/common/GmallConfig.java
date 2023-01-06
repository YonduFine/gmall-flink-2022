package com.ryleon.common;

/**
 * @author Lenovo
 */
public class GmallConfig {
    /**
     * Phoenix库名
     */
    public static final String HBASE_SCHEMA = "GMALL2212_REALTIME";

    /**
     * Phoenix驱动
     */
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    /**
     * Phoenix连接参数
     */
    public static final String PHOENIX_SERVER = "jdbc:phoenix:bigdata101,bigdata102,bigdata103:2181";

    /**
     * ClickHouse 驱动
     */
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    /**
     * ClickHouse 连接URL
     */
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://bigdata101:8123/gmall_2212";

    /**
     * Redis主机地址
     */
    public static final String REDIS_HOST = "bigdata101";

}
