package com.sixth.analysic.model.dim.base;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.StatsCommonDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:24 2018/8/20
 * @ 用于用户模块和浏览器模块的map和reduce阶段的输出的key和value的类型
 */
public class StatsUserDimension extends BaseStatsDimension {
    private BrowserDimension browserDimension = new BrowserDimension();
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(DateDimension dateDimension, PlatformDimension platformDimension,
                              KpiDimension kpiDimension) {

    }

    public StatsUserDimension(BrowserDimension browserDimension,
                              StatsCommonDimension statsCommonDimension) {
        this.browserDimension = browserDimension;
        this.statsCommonDimension = statsCommonDimension;
    }

    public static StatsUserDimension clone(StatsUserDimension dimension) {
        BrowserDimension browserDimension =
                new BrowserDimension(dimension.browserDimension.getBrowserName(),
                        dimension.browserDimension.getBrowserVersion());
        StatsCommonDimension statsCommonDimension =
                new StatsCommonDimension(dimension.statsCommonDimension.dateDimension,
                        dimension.statsCommonDimension.platformDimension,
                        dimension.statsCommonDimension.kpiDimension);

        return new StatsUserDimension(browserDimension, statsCommonDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.browserDimension.compareTo(other.browserDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.statsCommonDimension.compareTo(other.statsCommonDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 对象的序列化，调用对象本身的write方法，因为继承了Writable
        this.browserDimension.write(dataOutput);
        this.statsCommonDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 对象的readFields方法
        this.browserDimension.readFields(dataInput);
        this.statsCommonDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(browserDimension, that.browserDimension) &&
                Objects.equals(statsCommonDimension, that.statsCommonDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(browserDimension, statsCommonDimension);
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }
}
