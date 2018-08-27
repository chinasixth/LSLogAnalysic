package com.sixth.common;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:40 2018/8/20
 * @ kpi的枚举，新增的用户、订单数量等指标
 */
public enum KpiType {
    // 忽略其他条件，只统计新增用户
    NEW_USER("new_user"),
    // 按照浏览器分类统计用户
    BROWSER_NEW_USER("browser_new_user"),
    // 活跃用户
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    // 活跃会员
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    // 新增会员
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    // 会话
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    // hourly统计活跃用户
    HOURLY_ACTIVE_USER("hourly_active_user"),
    BROWSER_PAGEVIEW("browser_pageview"),
    // 地域维度下的统计
    LOCAL("local"),
    ;

    public String kpiName; // 属性

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    KpiType() {
    }

    public static KpiType valueOfType(String kpiName) {
        for (KpiType kpi : values()) {
            if (kpiName.equals(kpi.kpiName)) {
                return kpi;
            }
        }
        return null;
    }
}
