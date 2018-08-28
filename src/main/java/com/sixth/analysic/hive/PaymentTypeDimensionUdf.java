package com.sixth.analysic.hive;

import com.sixth.analysic.model.dim.base.PaymentTypeDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.PaymentTypeEnum;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:11 2018/8/27
 * @
 */
public class PaymentTypeDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String paymentType) {
        if (StringUtils.isEmpty(paymentType)) {
            paymentType = TimeUtil.getYesterday();
        }
        int id = -1;
        try {
            PaymentTypeDimension paymentTypeDimension = new PaymentTypeDimension(paymentType);
            id = convert.getDimensionByValue(paymentTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PaymentTypeDimensionUdf().evaluate(PaymentTypeEnum.AIRPAY.paymentName));
    }
}
