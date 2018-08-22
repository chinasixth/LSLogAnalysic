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
    BROWSER_NEW_USER("browser_new_user");

    public String kpiName; // 属性

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    KpiType() {
    }

    public static KpiType valueOfType(String kpiName){
        for (KpiType kpi : values()) {
            if (kpiName.equals(kpi.kpiName)) {
                return kpi;
            }
        }
        return null;
    }
}
