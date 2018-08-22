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
 * <p>
 *
 * value的输出：2018-08-20 website 2000 mapWritable(-1, 2)
 *
 * 符合我们指定的platform、date、browser、kpi的所有的数据（TimeOutputValue）都有相同的key，
 * 而value中的uuid是我们需要的，将所有的uuid进行去重，得到的就是新增用户数
 * key相当于是我们查询指标指定的条件，所以，在map阶段构建并输出的key，同样作为reduce阶段的key
 *
 * reduce阶段输出的value，这个比较讲究，value中得包含指标名，以及指标的结果，
 * 就本次指标来说，value要包含new_user(即kpi)，统计的结果（uuid去重后的结果）
 * 这里我们使用自定义的类来封装value的值，同时为了做到健壮性，考虑统计结果可能是多个字段，
 * 所以，我们用map来封装
 */
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(NewUserReducer.class);
//    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<>(); // 用于uuid去重统计

    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
            throws IOException, InterruptedException {
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
//        if (key.getStatsCommonDimension()
//                .getKpiDimension()
//                .getKpiName()
//                .equals(KpiType.valueOfType(KpiType.NEW_USER.kpiName))) {
//            this.v.setKpi(KpiType.NEW_USER);
//        }
        if(key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.NEW_USER.kpiName)){
            this.v.setKpi(KpiType.NEW_USER);
        }
        // 输出
        context.write(key, v);
    }
}
