package com.sixth.analysic.model.dim;

import com.sixth.analysic.model.dim.base.BaseDimension;
import com.sixth.analysic.model.dim.base.DateDimension;
import com.sixth.analysic.model.dim.base.KpiDimension;
import com.sixth.analysic.model.dim.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:59 2018/8/20
 * @ map阶段和reduce阶段输出的key的公共维度类型的封装
 */
public class StatsCommonDimension extends BaseStatsDimension {
    public DateDimension dateDimension = new DateDimension();
    public PlatformDimension platformDimension = new PlatformDimension();
    public KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    public StatsCommonDimension() {
    }

    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        PlatformDimension platformDimension = new PlatformDimension(dimension.platformDimension.getPlatformName());
        DateDimension dateDimension = new DateDimension(dimension.dateDimension.getYear(), dimension.dateDimension.getSeason(),
                dimension.dateDimension.getMonth(), dimension.dateDimension.getWeek(),
                dimension.dateDimension.getDay(), dimension.dateDimension.getType(),
                dimension.dateDimension.getCalendar());
        KpiDimension kpiDimension = new KpiDimension(dimension.kpiDimension.getKpiName());
        return new StatsCommonDimension(dateDimension, platformDimension, kpiDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.platformDimension.compareTo(other.platformDimension);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.dateDimension.compareTo(other.dateDimension);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.kpiDimension.compareTo(other.kpiDimension);
        if (tmp != 0) {
            return tmp;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 对对象的输入
        this.dateDimension.write(dataOutput);
        this.platformDimension.write(dataOutput);
        this.kpiDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 对对象输出
        this.dateDimension.readFields(dataInput);
        this.platformDimension.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
