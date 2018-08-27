package com.sixth.analysic.mr.session;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:53 2018/8/20
 * @ 会话个数和会话长度
 * 在某一个日期某一个平台（某一个浏览器）下的会话个数和会话长度
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(SessionReducer.class);
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<>(); // 用于uuid去重统计

    private MapWritable map = new MapWritable();
    private Map<String, List<Long>> sessionMap = new HashMap<>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
            throws IOException, InterruptedException {
        this.unique.clear();
        this.sessionMap.clear();
        // 循环，将sessionId添加到set中，实现去重
        for (TimeOutputValue tv : values) {
            this.unique.add(tv.getId());
            if (this.sessionMap.containsKey(tv.getId())) {
                this.sessionMap.get(tv.getId()).add(tv.getTime());
            } else {
                List<Long> li = new ArrayList<>();
                li.add(tv.getTime());
                this.sessionMap.put(tv.getId(), li);
            }
        }

        int sessionLength = 0;
        for (Map.Entry<String, List<Long>> entry : sessionMap.entrySet()) {
            if (entry.getValue().size() > 1) {
                sessionLength += (Collections.max(entry.getValue()) - Collections.min(entry.getValue()));
            }
        }

        // 构造输出的value，进来的key就是要输出的key，所以不用构建
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        map.put(new IntWritable(-2), new IntWritable(sessionLength % 1000 == 0 ? sessionLength : sessionLength + 1));

        this.v.setValue(map);
        // 设置kpi
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, v);
    }
}
