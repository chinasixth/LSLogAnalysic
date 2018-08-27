package com.sixth.analysic.mr.am;

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
public class ActiveMemberWriter implements IOutputWriter {

    @Override
    public void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;
        int activeMember = ((IntWritable) textOutputValue.getValue().get(new IntWritable(-1))).get();
        try {
            //
            int i = 0;
            ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
            if(statsUserDimension.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.BROWSER_ACTIVE_MEMBER.kpiName)){
                ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
            }
            ps.setInt(++i, activeMember);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, activeMember);

            //添加到批处理中
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
