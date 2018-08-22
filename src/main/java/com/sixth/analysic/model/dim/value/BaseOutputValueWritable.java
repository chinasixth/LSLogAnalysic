package com.sixth.analysic.model.dim.value;

import com.sixth.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:38 2018/8/20
 * @ map和reduce阶段输出的value的顶级父类
 */
public abstract class BaseOutputValueWritable implements Writable {
    public abstract KpiType getKpi();
}
