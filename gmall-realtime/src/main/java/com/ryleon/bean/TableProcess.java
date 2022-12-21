package com.ryleon.bean;

import lombok.Data;

@Data
public class TableProcess {

    //来源表
    private String sourceTable;
    //输出表
    private String sinkTable;
    //输出字段
    private String sinkColumns;
    //主键字段
    private String sinkPk;
    //建表扩展
    private String sinkExtend;

}
