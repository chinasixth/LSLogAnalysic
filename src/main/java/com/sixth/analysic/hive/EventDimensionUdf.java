package com.sixth.analysic.hive;

import com.sixth.analysic.model.dim.base.EventDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:53 2018/8/26
 * @ 获取事件维度的id
 */
public class EventDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String category, String action) {
        if (StringUtils.isEmpty(category)) {
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(action)) {
            action = GlobalConstants.DEFAULT_VALUE;
        }

        int id = 0;
        try {
            EventDimension eventDimension = new EventDimension(category, action);
            id = convert.getDimensionByValue(eventDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
