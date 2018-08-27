package com.sixth.analysic.mr.pv;

import com.sixth.analysic.model.dim.StatsCommonDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.base.*;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
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
 * @ Date   ：Created in 10:13 2018/8/25
 * @
 */
public class PVMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(PVMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension browserPvKpi = new KpiDimension(KpiType.BROWSER_PAGEVIEW.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String serverTime = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String url =  Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_CURRENT_URL)));
        String platform = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        System.out.println("手动打印url: " + url);
        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(url)) {
            LOGGER.warn("serverTime || url is null. serverTime: " + serverTime + "  url: " + url);
            return;
        }

        this.v.setId(url);
        long longOfServerTime = Long.parseLong(serverTime);
        this.v.setTime(longOfServerTime);

        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(browserPvKpi);
        for (PlatformDimension platformDimension : platformDimensions) {
            statsCommonDimension.setPlatformDimension(platformDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            for (BrowserDimension browserDimension : browserDimensions) {
                this.k.setBrowserDimension(browserDimension);
                context.write(this.k, this.v);
            }
        }
    }
}
