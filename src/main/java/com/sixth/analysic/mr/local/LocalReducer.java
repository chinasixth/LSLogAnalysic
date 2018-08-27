package com.sixth.analysic.mr.local;

import com.sixth.analysic.model.dim.StatsLocationDimension;
import com.sixth.analysic.model.dim.value.map.LocalMapOutputValue;
import com.sixth.analysic.model.dim.value.reduce.LocalReduceOutputValue;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:31 2018/8/25
 * @
 */
public class LocalReducer extends Reducer<StatsLocationDimension, LocalMapOutputValue, StatsLocationDimension, LocalReduceOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(LocalReducer.class);
    private LocalReduceOutputValue v = new LocalReduceOutputValue();
    private Set<String> unique = new HashSet<>(); // 用于uuid去重
    private Map<String, Integer> sessionMap = new HashMap<>();

    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<LocalMapOutputValue> values, Context context) {
        try {
            this.unique.clear();
            this.sessionMap.clear();
            for (LocalMapOutputValue lv : values) {
                this.unique.add(lv.getUid());
                if (sessionMap.containsKey(lv.getSid())) {
                    this.sessionMap.put(lv.getSid(), 2); // 非跳出会话
                } else {
                    this.sessionMap.put(lv.getSid(), 1); // 跳出会话个数
                }
            }

            this.v.setAus(this.unique.size());
            this.v.setSessions(this.map.size());
            int bounceSessions = 0;
            for (Map.Entry<String, Integer> entry : sessionMap.entrySet()) {
                if (entry.getValue() == 1) {
                    bounceSessions++;
                }
            }
            this.v.setBounceSessions(bounceSessions);
            this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
            context.write(key, this.v);
        } catch (Exception e) {
            LOGGER.warn("执行localReducer失败", e);
        }
    }
}
