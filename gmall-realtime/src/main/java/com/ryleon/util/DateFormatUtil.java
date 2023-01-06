package com.ryleon.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Long toTs(String dtStr, boolean isFull) {
        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    /**
     * <p>返回指定天对应的秒数</p>
     *
     * @param day 天数
     * @return int
     */
    public static int days(int day) {
        return day * 24 * 60 * 60;
    }

    /**
     * <p>返回指定小时的秒数</p>
     *
     * @param hour 小时
     * @return int
     */
    public static int hours(int hour) {
        return hour * 60 * 60;
    }

    /**
     * <p>返回指定分钟的秒数</p>
     *
     * @param minute 分钟
     * @return int
     */
    public static int minutes(int minute) {
        return minute * 60;
    }

}
