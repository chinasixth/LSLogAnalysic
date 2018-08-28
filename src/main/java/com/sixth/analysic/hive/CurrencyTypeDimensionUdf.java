package com.sixth.analysic.hive;

import com.sixth.analysic.model.dim.base.CurrencyTypeDimension;
import com.sixth.analysic.model.dim.base.DateDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.CurrencyTypeEnum;
import com.sixth.common.DateEnum;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:11 2018/8/27
 * @
 */
public class CurrencyTypeDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String currencyType) {
        if (StringUtils.isEmpty(currencyType)) {
            currencyType = TimeUtil.getYesterday();
        }
        int id = -1;
        try {
            CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(currencyType);
            id = convert.getDimensionByValue(currencyTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUdf().evaluate(CurrencyTypeEnum.CHINESE.currencyType));
    }
}
