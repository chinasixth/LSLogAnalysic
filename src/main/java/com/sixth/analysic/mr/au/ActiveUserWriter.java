package com.sixth.analysic.mr.au;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.analysic.mr.IOutputWriter;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.common.GlobalConstants;
import com.sixth.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:20 2018/8/21
 * @
 */
public class ActiveUserWriter implements IOutputWriter {

    @Override
    public void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;
        try {
            //
            int i = 0;
            KpiType kpi = textOutputValue.getKpi();
            switch (kpi) {
                case ACTIVE_USER:
                case BROWSER_ACTIVE_USER:
                    int activeUser = ((IntWritable) textOutputValue.getValue().get(new IntWritable(-1))).get();
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                    if (statsUserDimension.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.BROWSER_ACTIVE_USER.kpiName)) {
                        ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
                    }
                    ps.setInt(++i, activeUser);
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(++i, activeUser);
                    break;
                case HOURLY_ACTIVE_USER:
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getKpiDimension()));
                    for (i++; i < 28; i++) {
                        int v = ((IntWritable) textOutputValue.getValue().get(new IntWritable(i - 4))).get();
                        ps.setInt(i, v);
                        ps.setInt(i + 25, v);
                    }
                    ps.setString(i, conf.get(GlobalConstants.RUNNING_DATE));
                    break;
                default:
                    throw new RuntimeException("活跃用户和小时统计活跃用户失败");
            }
            //添加到批处理中
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
