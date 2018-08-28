package com.sixth.common;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:26 2018/8/27
 * @
 */
public enum CurrencyTypeEnum {
    AMERICAM("american"),
    CHINESE("chinese"),
    JAJPANISE("japanese"),;
    public String currencyType;

    CurrencyTypeEnum(String currencyType) {
        this.currencyType = currencyType;
    }

    CurrencyTypeEnum() {
    }

    public static CurrencyTypeEnum valueOfType(String currencyType) {
        for (CurrencyTypeEnum currencyTypeEnum : values()) {
            if (currencyType.equals(currencyTypeEnum.currencyType)) {
                return currencyTypeEnum;
            }
        }
        return null;
    }
}
