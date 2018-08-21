package com.sixth.common;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:04 2018/8/20
 * @ 时间枚举
 */
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String dateType;

    DateEnum() {
    }

    DateEnum(String dateType) {
        this.dateType = dateType;
    }

    public static DateEnum valueOfType(String type){
        for (DateEnum dateEnum : values()) {
            if(dateEnum.dateType.equals(type)){
                return dateEnum;
            }
        }
        return null;
    }
}
