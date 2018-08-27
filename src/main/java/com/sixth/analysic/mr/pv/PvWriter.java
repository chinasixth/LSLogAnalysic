package com.sixth.analysic.mr.pv;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.analysic.mr.IOutputWriter;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:23 2018/8/25
 * @
 */
public class PvWriter implements IOutputWriter {
    private static final Logger LOGGER = Logger.getLogger(PvWriter.class);

    @Override
    public void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;
        int i = 0;
        try {
            int pv = ((IntWritable) textOutputValue.getValue().get(new IntWritable(-1))).get();
            System.out.println("手动打印pv: " + pv);
            ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
            ps.setInt(++i, pv);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, pv);
            ps.addBatch();
        } catch (SQLException e) {
            LOGGER.warn("在pageview指标的writer方法中，为ps赋值失败");
        }
    }
}
