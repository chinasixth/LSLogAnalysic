package com.sixth.common;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:49 2018/8/27
 * @
 */
public enum PaymentTypeEnum {

    AIRPAY("airpay"),
    WEIXIN("weixin"),
    BANK_CARD("bank_card"),
    JINGDONG_PAY("jongdong_pay"),;

    public String paymentName;

    PaymentTypeEnum() {
    }

    PaymentTypeEnum(String paymentName) {
        this.paymentName = paymentName;
    }

    public static PaymentTypeEnum valueOfType(String paymentType) {
        for (PaymentTypeEnum paymentTypeEnum : values()) {
            if (paymentType.equals(paymentTypeEnum.paymentName)) {
                return paymentTypeEnum;
            }
        }
        return null;
    }
}
