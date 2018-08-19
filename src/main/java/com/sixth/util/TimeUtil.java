package com.sixth.util;

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
}
