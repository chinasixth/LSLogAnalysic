package com.sixth.analysic.model.dim;

import com.sixth.analysic.model.dim.base.BaseDimension;
import com.sixth.analysic.model.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:24 2018/8/20
 * @ 用于用户模块和浏览器模块的map和reduce阶段的输出的key和value的类型
 */
public class StatsLocationDimension extends BaseStatsDimension {
    private LocationDimension locationDimension = new LocationDimension();
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(LocationDimension locationDimension,
                                  StatsCommonDimension statsCommonDimension) {
        this.locationDimension = locationDimension;
        this.statsCommonDimension = statsCommonDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.locationDimension.compareTo(other.locationDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.statsCommonDimension.compareTo(other.statsCommonDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 对象的序列化，调用对象本身的write方法，因为继承了Writable
        this.locationDimension.write(dataOutput);
        this.statsCommonDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 对象的readFields方法
        this.locationDimension.readFields(dataInput);
        this.statsCommonDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsLocationDimension that = (StatsLocationDimension) o;
        return Objects.equals(locationDimension, that.locationDimension) &&
                Objects.equals(statsCommonDimension, that.statsCommonDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(locationDimension, statsCommonDimension);
    }

    public StatsCommonDimension getStatsCommonDimension() {

        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {

        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }
}
