package com.sixth.analysic.mr.nm;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:53 2018/8/20
 * @ 活跃用户的输出
 */
public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(NewMemberReducer.class);
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
        // 将新增会员id去重以后，需要添加到mysql数据库member_info表中，下次就是老会员了
        for (String memberId : unique) {
            map.put(new IntWritable(-2), new Text(memberId));
            this.v.setValue(map);
            this.v.setKpi(KpiType.MEMBER_INFO);
            // 专门用于存储新增会员的
            context.write(key, v);
        }

        // 构造输出的value，进来的key就是要输出的key，所以不用构建
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        this.v.setValue(map);
        // 设置kpi
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, v);
    }
}
