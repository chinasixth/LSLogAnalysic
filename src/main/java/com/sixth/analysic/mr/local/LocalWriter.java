package com.sixth.analysic.mr.local;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.StatsLocationDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.model.dim.value.reduce.LocalReduceOutputValue;
import com.sixth.analysic.mr.IOutputWriter;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.common.GlobalConstants;
import com.sixth.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:17 2018/8/26
 * @ 为ps赋值
 */
public class LocalWriter implements IOutputWriter {
    private static final Logger LOGGER = Logger.getLogger(LocalWriter.class);

    @Override
    public void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        int i = 0;
        try {
            StatsLocationDimension statsLocationDimension = (StatsLocationDimension) key;
            LocalReduceOutputValue localReduceOutputValue = (LocalReduceOutputValue) value;
            KpiType kpi = localReduceOutputValue.getKpi();
            int aus = localReduceOutputValue.getAus();
            int sessions = localReduceOutputValue.getSessions();
            int bounceSessions = localReduceOutputValue.getBounceSessions();

            ps.setInt(++i, convert.getDimensionByValue(statsLocationDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, convert.getDimensionByValue(statsLocationDimension.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i, convert.getDimensionByValue(statsLocationDimension.getLocationDimension()));
            ps.setInt(++i, aus);
            ps.setInt(++i, sessions);
            ps.setInt(++i, bounceSessions);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, aus);
            ps.setInt(++i, sessions);
            ps.setInt(++i, bounceSessions);

            ps.addBatch();
        } catch (SQLException e) {
            LOGGER.warn("运行localWriter给ps赋值失败", e);
        }

    }
}
