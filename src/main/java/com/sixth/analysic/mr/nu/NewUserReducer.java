package com.sixth.analysic.mr.nu;

import com.sixth.analysic.model.dim.base.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:53 2018/8/20
 * @ 新增用户的reducer
 *
 *   value的输出：2018-08-20 website 2000
 */
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(NewUserReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<>(); // 用于uuid去重统计

    MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空Set
        this.unique.clear();
        // 循环，将uuid添加到set中，实现去重
        for (TimeOutputValue tv : values) {
            this.unique.add(tv.getId());
        }
        // 构造输出的value，进来的key就是要输出的key，所以不用构建
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        this.v.setValue(map);
        // 设置kpi
        if(key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.valueOfType(KpiType.NEW_USER.kpiName))){
            this.v.setKpi(KpiType.NEW_USER);
        }
        // 输出
        context.write(k, v);
    }
}
