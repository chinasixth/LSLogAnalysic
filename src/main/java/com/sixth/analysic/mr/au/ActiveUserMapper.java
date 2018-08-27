package com.sixth.analysic.mr.au;

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
 * @ Date   ：Created in 11:53 2018/8/20
 * @ 活跃用户：将所有的数据按照uuid去重，得到的就是活跃用户
 */
public class ActiveUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(ActiveUserMapper.class);
    // 列簇
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    // 用户模块和浏览器模块map和reduce阶段输出的key
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.kpiName);
    private KpiDimension browserActiveMemberKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);
    private KpiDimension hourlyActiveUser = new KpiDimension(KpiType.HOURLY_ACTIVE_USER.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String serverTime = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String uuid = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)));
        String platformName = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        // 确定一下我们拿到的uuid和serverTime不能为空
        if (StringUtils.isEmpty(uuid) || StringUtils.isEmpty(serverTime)) {
            LOGGER.warn("uuid && serverTime is not null. uuid:" + uuid + "  serverTime: " + serverTime);
            return;
        }
        // 构造输出的value
        this.v.setId(uuid);
        long longOfServerTime = Long.parseLong(serverTime);
        this.v.setTime(longOfServerTime);
        // 构造输出的key
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");

        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        // 循环平台维度输出
        for (PlatformDimension pl : platformDimensions) {
            statsCommonDimension.setPlatformDimension(pl);
            statsCommonDimension.setKpiDimension(activeUserKpi);
            // 设置默认的浏览器维度
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            // 先输出activeUserKpi，用不到browser，用到browser的就不是activeUserKpi了
            context.write(this.k, this.v);

            // 输出hourlyActiveUserKpi
            statsCommonDimension.setKpiDimension(hourlyActiveUser);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(k, v);

            // 输出browserActiveMemberKpi
            for (BrowserDimension browserDimension : browserDimensions) {
                this.k.setBrowserDimension(browserDimension);
                // 注意修改kpi
                statsCommonDimension.setKpiDimension(browserActiveMemberKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                context.write(this.k, this.v);
            }
        }
    }
}
