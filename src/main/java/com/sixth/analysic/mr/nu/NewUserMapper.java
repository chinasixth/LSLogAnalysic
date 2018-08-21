package com.sixth.analysic.mr.nu;

import com.sixth.analysic.model.dim.StatsCommonDimension;
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
 * @ key  : 是维度，用来将数据存到到不同的表中
 * value: 是具体的数据，可以封装成对象
 * <p>
 * 统计新增的用户，launch事件中uuid的去重个数
 * value中存储的是uuid和对应的个数
 *
 * 我们要统计新增的用户模块
 * 数据是从hbase中拿过来的，是已经将日志解析好的数据
 * 要想达到我们的目的，需要根据日志的en字段(时间类型)来进行判断
 * 对于拿到的每一个row-key对应的数据，取出serverTime、uuid、platform，并使用这三个构建输出的key，
 * 我们自己构建的key中有BrowserDimension，然而这个属性在统计新增用户中没有实质性的作用，所以给定一个默认值就行
 * 为什么要选择date、kpi、platform来构建key？
 * 一个map对应一条row-key？一条row-key产生两条结果数据？如：一条是platform为website，另一条数据是platform为all
 *
 *
 */
public class NewUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(NewUserMapper.class);
    // 列簇
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    // 用户模块和浏览器模块map和reduce阶段输出的key
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        // 从hbase中读取数据，可能是因为继承的是TableMapper，所以知道value就是hbase表中的数据
        // 获取数据时指定列簇名和列名
        String serverTime = value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)).toString();
        String uuid = value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)).toString();
        String platformName = value.getValue(family,
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)).toString();

        if (StringUtils.isEmpty(uuid) || StringUtils.isEmpty(serverTime)) {
            LOGGER.warn("uuid && serverTime is not null. uuid:" + uuid + "  serverTime: " + serverTime);
        }
        // 构造输出的value
        this.v.setId(uuid);
        long longOfServerTime = Long.valueOf(serverTime);
        this.v.setTime(longOfServerTime);
        // 构造输出的key
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");

        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        // 循环平台维度输出
        for (PlatformDimension pl : platformDimensions) {
            statsCommonDimension.setPlatformDimension(pl);
            statsCommonDimension.setKpiDimension(newUserKpi);
            // 设置默认的浏览器维度
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k, this.v);
        }
    }
}
