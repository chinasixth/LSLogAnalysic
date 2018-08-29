package com.sixth.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:49 2018/8/15
 * @ 解析UserAgent代理对象
 */
public class UserAgentUtil {

    private static final Logger logger = Logger.getLogger(UserAgentUtil.class);

    private static UASparser ua = null;

    // 初始化
    static {
        try {
            ua = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     *  解析浏览器代理对象
     * */
    public static UserAgentInfo parseUserAgent(String agent) {
        UserAgentInfo uaInfo = new UserAgentInfo();
        if (StringUtils.isEmpty(agent)) {
            logger.warn("agent is null");
            return null;
        }
        // 正常解析
        try {
            cz.mallat.uasparser.UserAgentInfo info = ua.parse(agent);
            uaInfo.setBrowserName(info.getUaFamily());
            uaInfo.setBrowserVersion(info.getBrowserVersionInfo());
            uaInfo.setOsName(info.getOsFamily());
            uaInfo.setOsVersion(info.getOsName());

        } catch (Exception e) {
            logger.warn("userAgent parse exception");
        }

        return uaInfo;
    }

    /*
     * 用于封装解析出来的字段，浏览器名、版本、操作系统名、版本
     * */
    public static class UserAgentInfo {
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }
    }
}
