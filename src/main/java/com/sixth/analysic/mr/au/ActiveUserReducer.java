package com.sixth.analysic.mr.au;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.common.DateEnum;
import com.sixth.common.KpiType;
import com.sixth.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:53 2018/8/20
 * @ 活跃用户的输出
 * 增加一个按小时分析活跃用户数据
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(ActiveUserReducer.class);
    //    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<>(); // 用于uuid去重统计
    private Map<Integer, HashSet<String>> hoursMap = new HashMap<>();
    private MapWritable map = new MapWritable();
    private MapWritable hourlyWritable = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 初始化小时数
        for (int i = 0; i<  24; i++){
            hoursMap.put(i, new HashSet<>());
            hourlyWritable.put(new IntWritable(i), new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
            throws IOException, InterruptedException {
        try {
            if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.HOURLY_ACTIVE_USER.kpiName)) {
                for (TimeOutputValue tv : values) {
                    // 拿到这条数据的时间戳，而且能得到一个具体的小时数
                    int hours = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR) - 1;
                    this.hoursMap.get(hours).add(tv.getId());
                }
                this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
                // 循环取size
                for (Map.Entry<Integer, HashSet<String>> entry : hoursMap.entrySet()) {
                    this.hourlyWritable.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue().size()));
                }
                this.v.setValue(hourlyWritable);
                context.write(key, this.v);
            } else {
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
                this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
                // 输出
                context.write(key, v);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.unique.clear();
            this.hoursMap.clear();
            this.hourlyWritable.clear();
            for (int i = 0; i<  24; i++){
                hoursMap.put(i, new HashSet<>());
                hourlyWritable.put(new IntWritable(i), new IntWritable(0));
            }
        }
    }
}
