package com.sixth.analysic.mr.service;

import com.sixth.analysic.model.dim.base.BaseDimension;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:33 2018/8/21
 * @ 根据各个基础维度对象获取在数据库中对应的维度id
 */
public interface IDimensionConvert {
    /*
    * 根据维度获取id
    * */
    int getDimensionByValue(BaseDimension dimension);
}
