package com.ryleon.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ALiang
 * @date 2023-01-04
 * @effect 适用于任何采用JDBC协议的数据库curd操作
 */
public class JdbcUtil {
    /**
     * <p>根据查询语句对指定得数据进行查询并返回一个查询结果并封装为指定类型得JavaBean</p>
     * <p>查询结果可以是单行单列、单行多列、多行多列、多行多列</p>
     *
     * @param connection        数据库连接对象
     * @param querySql          查询语句
     * @param clz               返回结果类型的JavaBean
     * @param underScoreToCamel 下划线名字转驼峰命名
     * @param <T>               JavaBean
     * @return List<T>
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        List<T> resultList = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        ResultSet resultSet = preparedStatement.executeQuery();
        // 获取元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 通过循环赋值将结果的值转换为T对象并加入List
        while (resultSet.next()) {
            // 创建T对象
            T t = clz.newInstance();
            for (int i = 0; i < columnCount; i++) {
                // 获取列名与列值
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);
                // 判断是否需要转换命名方式
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                // 将赋值给t
                BeanUtils.setProperty(t, columnName, value);
            }
            // 将结果加入结果集中
            resultList.add(t);
        }
        resultSet.close();
        preparedStatement.close();
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/shixun_demo?useSSL=false&charset=utf8&serverTimezone=UTC", "root", "0428");
        List<JSONObject> queryList = queryList(connection, "select *  from job_degree_analysis", JSONObject.class, true);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
