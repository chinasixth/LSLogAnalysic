package com.sixth.common;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:48 2018/8/17
 * @ 日志的常量类
 */
public class EventLogConstants {

    /*
    * 事件的枚举
    * 定义一个枚举类
    * */
    public enum EventEnum{
        // 限定枚举的几种类型，当选定一种类型的时候，就将使用后面的参数来实例化这个枚举类
        // launch事件，第一次访问网站的时候触发，可以用来统计新增用户
        LAUNCH(1, "launch event", "e_l"),
        // pageView事件，当用于访问页面或者刷新页面的时候触发该事件，
        // 这个事件会记录很多的日志信息，比如ip，浏览器信息等
        PAGEVIEW(2, "page view event", "e_pv"),
        // 当用户下订单的时候触发该事件
        CHARGEREQUEST(3, "charge request event", "e_crt"),
        // 订单成功时触发
        CHARGESUCCESS(4, "charge success event", "e_cs"),
        // 撤销订单时触发
        CHARGEREFUND(5, "charge refund event", "e_cr"),
        // 当用户访问定制业务时，会触发该事件
        EVENT(6, "event", "e_e");

        public final int id; // 事件的id
        public final String name;
        public final String alias; // 别名

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /*
        * 根据别名获取枚举
        * */
        public static EventEnum valueOfAlias(String alias) {
            // values()是自带的，将上面的类型遍历
            for(EventEnum event: values()){
                if(alias.equals(event.alias)){
                    return event;
                }
            }
            return null;
        }
    }

    public static final String LOG_SEPARTOR ="\\^A";
    public static final String HBASE_TABLE_NAME = "logs";
    public static final String HBASE_COLUMN_FAMILY = "info";

    /*
    * 日志列
    * */
    public static final String EVENT_COLUMN_NAME_VERSION = "ver";

    public static final String EVENT_COLUMN_NAME_SERVER_TIME = "s_time";

    public static final String EVENT_COLUMN_NAME_EVENT_NAME = "en";

    public static final String EVENT_COLUMN_NAME_UUID = "u_ud";

    public static final String EVENT_COLUMN_NAME_MEMBER_ID = "u_mid";

    public static final String EVENT_COLUMN_NAME_SESSION_ID = "u_sd";

    public static final String EVENT_COLUMN_NAME_CLIENT_TIME = "c_time";

    public static final String EVENT_COLUMN_NAME_LANGUAGE = "l";

    public static final String EVENT_COLUMN_NAME_USERAGENT = "b_iev";

    public static final String EVENT_COLUMN_NAME_RESOLUTION = "b_rst";

    public static final String EVENT_COLUMN_NAME_CURRENT_URL = "p_url";

    public static final String EVENT_COLUMN_NAME_PREFFER_URL = "p_ref";

    public static final String EVENT_COLUMN_NAME_TITLE = "tt";

    public static final String EVENT_COLUMN_NAME_PLATFORM = "pl";

    public static final String EVENT_COLUMN_NAME_IP = "ip";


    /**
     * 和订单相关
     */
    public static final String EVENT_COLUMN_NAME_ORDER_ID = "oid";

    public static final String EVENT_COLUMN_NAME_ORDER_NAME = "on";

    public static final String EVENT_COLUMN_NAME_CURRENCY_AMOUTN = "cua";

    public static final String EVENT_COLUMN_NAME_CURRENCY_TYPE = "cut";

    public static final String EVENT_COLUMN_NAME_PAYMENT_TYPE = "pt";


    /**
     * 事件相关
     * 点击：点击事件 category   点赞、收藏、喜欢、转发
     * 下单：下单事件
     */
    public static final String EVENT_COLUMN_NAME_EVENT_CATEGORY = "ca";

    public static final String EVENT_COLUMN_NAME_EVENT_ACTION = "ac";

    public static final String EVENT_COLUMN_NAME_EVENT_KV = "kv_";

    public static final String EVENT_COLUMN_NAME_EVENT_DURATION = "du";

    /**
     * 浏览器相关
     */

    public static final String EVENT_COLUMN_NAME_BROWSER_NAME = "browserName";

    public static final String EVENT_COLUMN_NAME_BROWSER_VERSION = "browserVersion";

    public static final String EVENT_COLUMN_NAME_OS_NAME = "osName";

    public static final String EVENT_COLUMN_NAME_OS_VERSION = "osVersion";

    /**
     * 地域相关
     */

    public static final String EVENT_COLUMN_NAME_COUNTRY = "country";

    public static final String EVENT_COLUMN_NAME_PROVINCE = "province";

    public static final String EVENT_COLUMN_NAME_CITY = "city";
}
