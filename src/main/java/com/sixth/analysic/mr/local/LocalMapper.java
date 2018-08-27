package com.sixth.analysic.mr.local;

import com.sixth.analysic.model.dim.StatsCommonDimension;
import com.sixth.analysic.model.dim.StatsLocationDimension;
import com.sixth.analysic.model.dim.base.DateDimension;
import com.sixth.analysic.model.dim.base.KpiDimension;
import com.sixth.analysic.model.dim.base.LocationDimension;
import com.sixth.analysic.model.dim.base.PlatformDimension;
import com.sixth.analysic.model.dim.value.map.LocalMapOutputValue;
import com.sixth.common.DateEnum;
import com.sixth.common.EventLogConstants;
import com.sixth.common.KpiType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:07 2018/8/25
 * @ 地域维度
 * 按照地域维度统计所有的活跃用户、非跳出会话数、跳出会话数
 */
public class LocalMapper extends TableMapper<StatsLocationDimension, LocalMapOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(LocalMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    private StatsLocationDimension k = new StatsLocationDimension();
    private LocalMapOutputValue v = new LocalMapOutputValue();
    private KpiDimension localKpi = new KpiDimension(KpiType.LOCAL.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String serverTime = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String uuid = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)));
        String sessionId = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SESSION_ID)));
        String platform = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String country = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY)));
        String province = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE)));
        String city = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_CITY)));

        if (StringUtils.isEmpty(serverTime)) {
            LOGGER.warn("serverTime is null. serverTime: " + serverTime);
            return;
        }

        long longOfServerTime = Long.parseLong(serverTime);
        this.v.setUid(uuid);
        this.v.setSid(sessionId);

        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        List<LocationDimension> locationDimensions = LocationDimension.buildList(country, province, city);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(localKpi);
        for (PlatformDimension platformDimension : platformDimensions) {
            statsCommonDimension.setPlatformDimension(platformDimension);
            for (LocationDimension locationDimension : locationDimensions) {
                this.k.setLocationDimension(locationDimension);
                context.write(this.k, this.v);
            }
        }
    }
}
