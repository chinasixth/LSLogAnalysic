package com.sixth.analysic.mr.pv;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:09 2018/8/25
 * @
 */
public class PVReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TextOutputValue> {
    private static final Logger LOGGER = Logger.getLogger(PVReducer.class);
    private TextOutputValue v = new TextOutputValue();
    private List<String> urlList = new ArrayList<>();
    private MapWritable urlMap = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        urlList.clear();
        for (TimeOutputValue tv : values) {
            urlList.add(tv.getId());
        }
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        urlMap.put(new IntWritable(-1), new IntWritable(this.urlList.size()));
        this.v.setValue(urlMap);
        context.write(key, this.v);
    }
}
