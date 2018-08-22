package com.sixth.analysic.mr;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.mr.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:28 2018/8/21
 * @ 自定义reduce阶段输出的格式类，为每一个sql语句赋值的接口
 */
public interface IOutputWriter {
    /*
    * 为每一个指标的sql语句赋值，也就是将value中的值拿出来用于赋值
    * */
    void writer(Configuration conf, BaseStatsDimension key, BaseOutputValueWritable value,
                PreparedStatement ps, IDimensionConvert convert);
}
