package com.sixth.analysic.mr.nm;

import com.sixth.analysic.model.dim.StatsCommonDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.base.*;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.common.DateEnum;
import com.sixth.common.EventLogConstants;
import com.sixth.common.GlobalConstants;
import com.sixth.common.KpiType;
import com.sixth.util.JdbcUtil;
import com.sixth.util.MemberUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:53 2018/8/20
 * @ 新增会员：对所有数据的mid进行去重，然后查询mysql中的member_info表中是否有这个会员id，如果有则是老会员，没有则是新会员
 */
public class NewMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(NewMemberMapper.class);
    // 列簇
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    // 用户模块和浏览器模块map和reduce阶段输出的key
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewMemberKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);

    private Connection conn = null;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        conn = JdbcUtil.getConn();
        MemberUtil.deleteMemberInfoByDate(conf.get(GlobalConstants.RUNNING_DATE), conn);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String serverTime = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String memberId = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID)));
        String platformName = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = new String(value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        // 确定一下我们拿到的uuid和serverTime不能为空
        if (StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serverTime)) {
            LOGGER.warn("memberId && serverTime is not null. memberId:" + memberId + "  serverTime: " + serverTime);
            return;
        }
        // 判断memberId是否是一个新的会员id，方法是查询数据库中的会员表
        if (!MemberUtil.isNewMember(conn, memberId)) {
            LOGGER.info("该memberId是一个老会员. memberId" + memberId);
            return;
        }

        // 构造输出的value
        this.v.setId(memberId);
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
            statsCommonDimension.setKpiDimension(newMemberKpi);
            // 设置默认的浏览器维度
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k, this.v);
            for (BrowserDimension browserDimension : browserDimensions) {
                this.k.setBrowserDimension(browserDimension);
                // 注意修改kpi
                statsCommonDimension.setKpiDimension(browserNewMemberKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                context.write(this.k, this.v);
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        JdbcUtil.close(conn, null, null);
    }
}
