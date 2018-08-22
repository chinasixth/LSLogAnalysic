package com.sixth.util;

import com.sixth.common.DateEnum;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:12 2018/8/17
 * @ 全局的时间工具类
 */
public class TimeUtil {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    /*
     * 判断时间是否有效
     * 用正则表达式判断 yyyy-MM-dd
     * */
    public static boolean isValidateDate(String date) {
        Matcher matcher = null;
        Boolean res = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (StringUtils.isNotEmpty(date)) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if (matcher != null) {
            res = matcher.matches();
        }

        return res;
    }

    /*
     * 默认获取昨天的日期 yyyy-MM-dd
     * */
    public static String getYesterday() {
        return getYesterday(DEFAULT_FORMAT);
    }

    /*
     * 指定格式
     * */
    public static String getYesterday(String parrern) {
        SimpleDateFormat sdf = new SimpleDateFormat(parrern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);

        return sdf.format(calendar.getTime());
    }

    /*
     * 将时间戳转换成日期
     * 根据指定时间戳返回默认格式的日期
     * */
    public static String parseLong2String(Long time) {

        return parseLong2String(time, DEFAULT_FORMAT);
    }

    /*
     * 将时间戳转换成指定格式的日期
     * */
    public static String parseLong2String(Long time, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);

        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /*
     * 将日期转换成时间戳
     * 根据指定日期返回默认格式的时间戳
     * */
    public static Long parseString2Long(String date) {
        return parseString2Long(date, DEFAULT_FORMAT);
    }

    /*
     * 将日期转换成指定格式的时间戳
     * */
    public static Long parseString2Long(String date, String pattern) {
        Date dt = null;

        try {
            dt = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dt.getTime();
    }

    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (type.equals(DateEnum.YEAR)) {
            return calendar.get(Calendar.YEAR);
        }
        if (type.equals(DateEnum.SEASON)) {
            int month = calendar.get(Calendar.MONDAY);
            return month % 3 == 0 ? month / 3 : (month / 3 + 1);
        }
        if (type.equals(DateEnum.MONTH)) {
            return calendar.get(Calendar.MONDAY);
        }
        if (type.equals(DateEnum.WEEK)) {
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }
        if (type.equals(DateEnum.DAY)) {
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if (type.equals(DateEnum.HOUR)) {
            return calendar.get(Calendar.HOUR_OF_DAY) + 1;
        }
        throw new RuntimeException("不支持该类型的日期信息获取.type" + type.dateType);
    }

    /*
     * 获取周第一天时间戳
     * */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        // 设置
        calendar.set(Calendar.DAY_OF_WEEK, 1); // 该周的第一天
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
