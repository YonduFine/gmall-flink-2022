package com.ryleon.util;

/**
 * @author ALiang
 * @date 2023-01-03
 * @effect FlinkSQL 时间数据类型TimestampLtz3时间比较类
 */
public class TimestampLtz3CompareUtil {
    /**
     * 比较两个时间得大小 若第一条数据得时间大则返回1
     * 两个时间相等返回0 第二条时间大返回-1
     *
     * @param timestamp1 时间1
     * @param timestamp2 时间2
     * @return 整型 1 0 -1
     */
    public static int compare(String timestamp1, String timestamp2) {
        // 数据格式 2022-04-01 10:20:47.302Z
        // 1. 去除末尾的时区标志，'Z' 表示 0 时区
        String cleanedTime1 = timestamp1.substring(0, timestamp1.length() - 1);
        String cleanedTime2 = timestamp2.substring(0, timestamp2.length() - 1);
        // 2. 提取小于 1秒的部分
        String[] timeArr1 = cleanedTime1.split("\\.");
        String[] timeArr2 = cleanedTime2.split("\\.");
        String microseconds1 = new StringBuilder(timeArr1[timeArr1.length - 1])
            .append("000").toString().substring(0, 3);
        String microseconds2 = new StringBuilder(timeArr2[timeArr2.length - 1])
            .append("000").toString().substring(0, 3);
        int micro1 = Integer.parseInt(microseconds1);
        int micro2 = Integer.parseInt(microseconds2);
        // 3. 提取 yyyy-MM-dd HH:mm:ss 的部分
        String date1 = timeArr1[0];
        String date2 = timeArr2[0];
        Long ts1 = DateFormatUtil.toTs(date1, true);
        Long ts2 = DateFormatUtil.toTs(date2, true);
        // 4. 获得精确到毫秒的时间戳
        long microTs1 = ts1 * 1000 + micro1;
        long microTs2 = ts2 * 1000 + micro2;

        long divTs = microTs1 - microTs2;

        return divTs < 0 ? -1 : divTs == 0 ? 0 : 1;
    }

    public static void main(String[] args) {
        System.out.println(compare("2023-01-03 11:10:55.060Z", "2023-01-03 11:10:55.05Z"));
    }

}
