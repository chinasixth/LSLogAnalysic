package com.sixth.analysic.mr.pv;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.mr.IOutputWriterFormat;
import com.sixth.common.EventLogConstants;
import com.sixth.common.GlobalConstants;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:24 2018/8/25
 * @
 */
public class PVRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(PVReducer.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.addResource("writer-mapping.xml");
        conf.addResource("output-mapping.xml");
        conf.addResource("total-mapping.xml");
        setArgs(conf, args);
        Job job = Job.getInstance(conf, "pageview");
        job.setJarByClass(PVRunner.class);
        // 设置向map输入的数据源，以及map阶段的输入
        TableMapReduceUtil.initTableMapperJob(this.buildScan(job),
                PVMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job, true);

        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);
        // 设置reduce阶段
        job.setReducerClass(PVReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TimeOutputValue.class);
        // 设置最后向mysql输出的类
        job.setOutputFormatClass(IOutputWriterFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private List<Scan> buildScan(Job job) {
        Configuration conf = job.getConfiguration();
        long startDate = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE));
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));

        FilterList fl = new FilterList();

        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_CURRENT_URL,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION
        };
        fl.addFilter(this.getColumnsFilter(columns));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        scan.setFilter(fl);
        List<Scan> li = new ArrayList<>();
        li.add(scan);
        return li;
    }

    private Filter getColumnsFilter(String[] columns) {
        int length = columns.length;
        byte[][] bytes = new byte[length][];
        for (int i = 0; i < length; i++) {
            bytes[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(bytes);
    }

    private void setArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                date = args[i + 1];
            }
        }
        if (StringUtils.isEmpty(date) || !TimeUtil.isValidateDate(date)) {
            date = TimeUtil.getYesterday();
        }
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new PVRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("运行pageview的runner失败", e);
        }
    }
}
