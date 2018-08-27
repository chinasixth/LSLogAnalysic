package com.sixth.analysic.mr.nm;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.analysic.mr.IOutputWriter;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.common.GlobalConstants;
import com.sixth.common.KpiType;
import com.sixth.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:20 2018/8/21
 * @
 */
public class NewMemberWriter implements IOutputWriter {

    @Override
    public void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;
        int i = 0;
        switch (value.getKpi()) {
            case NEW_MEMBER:
            case BROWSER_NEW_MEMBER:
                int newMember = ((IntWritable) textOutputValue.getValue().get(new IntWritable(-1))).get();
                try {
                    //

                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                    if (statsUserDimension.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.BROWSER_NEW_MEMBER.kpiName)) {
                        ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
                    }
                    ps.setInt(++i, newMember);
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(++i, newMember);

                    //添加到批处理中
                    ps.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                break;
            case MEMBER_INFO:
                Text memberId = (Text) textOutputValue.getValue().get(new IntWritable(-2));
                try {
                    ps.setString(++i, memberId.toString());
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setLong(++i, TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE)));
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new RuntimeException("新增会员设置ps失败");
        }
    }
}
