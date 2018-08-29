package com.sixth.analysic.hive;

import com.sixth.analysic.model.dim.base.PlatformDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:53 2018/8/26
 * @ 获取平台维度的id
 */
public class PlatformDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String platform) {
        if (StringUtils.isEmpty(platform)) {
            platform = GlobalConstants.DEFAULT_VALUE;
        }

        int id = -1;
        try {
            PlatformDimension platformDimension = new PlatformDimension(platform);
            id = convert.getDimensionByValue(platformDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUdf().evaluate("website"));
    }
}
