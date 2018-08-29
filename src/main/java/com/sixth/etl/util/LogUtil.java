package com.sixth.etl.util;

import com.sixth.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:41 2018/8/17
 * @ 整行日志的解析工具
 */
public class LogUtil {
    private static final Logger LOGGER = Logger.getLogger(LogUtil.class);

    /*
     * 单行日志的解析
     * */
    public static Map<String, String> handleLog(String log) {
        // 线程安全
        Map<String, String> info = new ConcurrentHashMap<>();
        if (StringUtils.isNotEmpty(log.trim())) {
            // 拆分单行日志
            String[] fields = log.split(EventLogConstants.LOG_SEPARTOR);
            if (fields.length == 4) {
                // 存储数据到info
                info.put(EventLogConstants.EVENT_COLUMN_NAME_IP, fields[0]);
                info.put(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME, fields[1].replaceAll("\\.", ""));

                // 处理参数列表
                handleParams(info, fields[3]);

                // 再处理ip
                handleIp(info);

                // 处理UserAgent
                handleUserAgent(info);
            }
        }

        return info;
    }

    /*
     * 处理userAgent
     * */
    private static void handleUserAgent(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT)) {
            UserAgentUtil.UserAgentInfo ua = UserAgentUtil.parseUserAgent(info.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT));
            if (ua != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, ua.getBrowserName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, ua.getBrowserVersion());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, ua.getOsName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, ua.getOsVersion());
            }
        }
    }

    private static void handleIp(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_IP)) {
            IPParseUtil.RegionInfo regionInfo = IPParseUtil.parserIp(info.get(EventLogConstants.EVENT_COLUMN_NAME_IP));

            if (regionInfo != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY, regionInfo.getCountry());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE, regionInfo.getProvince());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_CITY, regionInfo.getCity());
            }
        }
    }

    /*
     * 处理参数
     * */
    private static void handleParams(Map<String, String> info, String field) {
        if (StringUtils.isNotEmpty(field)) {
            int index = field.indexOf("?");
            if (index > 0) {
                String fields = field.substring(index + 1);
                String[] params = fields.split("&");

                for (String param : params) {
                    String[] kvs = param.split("=");
                    try {
                        String v = URLDecoder.decode(kvs[1], "utf-8");
                        String k = kvs[0];
                        if (StringUtils.isNotEmpty(k)) {
                            info.put(k, v);
                        }
                    } catch (UnsupportedEncodingException e) {
                        LOGGER.warn("url的解码异常");
                    }
                }
            }
        }
    }
}
