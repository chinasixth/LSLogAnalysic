package com.sixth.analysic.hive;

import com.sixth.analysic.model.dim.base.DateDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.DateEnum;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:53 2018/8/26
 * @ 获取时间维度的id
 */
public class DateDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String date) {
        if (StringUtils.isEmpty(date)) {
            date = TimeUtil.getYesterday();
        }
        int id = -1;
        try {
            DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.DAY);
            id = convert.getDimensionByValue(dateDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new DateDimensionUdf().evaluate("2018-08-20"));
    }
}
