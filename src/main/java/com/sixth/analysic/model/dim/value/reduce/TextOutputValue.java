package com.sixth.analysic.model.dim.value.reduce;

import com.sixth.analysic.model.dim.value.BaseOutputValueWriable;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:56 2018/8/20
 * @ 用户模块和浏览器模块reduce输出value的类型
 */
public class TextOutputValue extends BaseOutputValueWriable {
    private KpiType kpi;
    private MapWritable value = new MapWritable();

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 枚举类型的序列化和反序列化
        // 对象的序列化和反序列化
        // 要记住哦~
        WritableUtils.writeEnum(dataOutput, kpi);
        this.value.write(dataOutput);
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 读枚举的第二个参数
        WritableUtils.readEnum(dataInput, KpiType.class);
        this.value.readFields(dataInput);
    }

}
