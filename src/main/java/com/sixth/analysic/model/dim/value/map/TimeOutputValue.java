package com.sixth.analysic.model.dim.value.map;

import com.sixth.analysic.model.dim.value.BaseOutputValueWriable;
import com.sixth.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:46 2018/8/20
 * @ 用于map阶段的输出的value的类型
 */
public class TimeOutputValue extends BaseOutputValueWriable {
    private String id; // 泛指，有可能是uuid，mid等
    private long time; // 产生数据的时间戳

    @Override
    public KpiType getKpi() {
        // map阶段不用写kpi，因为map阶段有kpiDimension
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
