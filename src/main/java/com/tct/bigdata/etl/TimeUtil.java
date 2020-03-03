package com.tct.bigdata.etl;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

/**
 * 时间工具类
 */
public class TimeUtil {

    public static final Long ONE_DAY_MILLISECOND = 24L * 60 * 60 * 1000;

    public static final String DATE_FORMAT_YMD = "yyyy-MM-dd";

    public static Long getTodayStartTimestamp(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime().getTime();
    }

    public static Long getYesterdayStartTimestamp(){
        return getTodayStartTimestamp() - ONE_DAY_MILLISECOND;
    }

    public static String getYesterdayStartFormat(String format){
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        DateTime today = new DateTime();
        return dtf.print(today);
    }

    public static String getYesterdayStartFormat(){
        return getYesterdayStartFormat(DATE_FORMAT_YMD);
    }

    public static String formatDateByTimestamp(Long timestamp, String format){
        DateTime dateTime = new DateTime(timestamp);
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        return dtf.print(dateTime);
    }

    public static Long getTimestamp(String dateStr, String format){
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        DateTime dateTime = dtf.parseDateTime(dateStr);
        return dateTime.getMillis();
    }

    public static void main(String[] args){
        /*Long todayStartTimestamp = getYesterdayStartTimestamp();
        System.out.println(todayStartTimestamp);
        System.out.println(formatDateByTimestamp(todayStartTimestamp, DATE_FORMAT_YMD));*/
        System.out.println(getTimestamp("2020-02-21", "yyyy-MM-dd"));
    }
}
