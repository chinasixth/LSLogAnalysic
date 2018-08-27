package com.sixth.analysic.mr.local;

import com.google.common.collect.Lists;
import com.sixth.analysic.model.dim.StatsLocationDimension;
import com.sixth.analysic.model.dim.value.map.LocalMapOutputValue;
import com.sixth.analysic.model.dim.value.reduce.LocalReduceOutputValue;
import com.sixth.analysic.mr.IOutputWriterFormat;
import com.sixth.common.EventLogConstants;
import com.sixth.common.GlobalConstants;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
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

import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:48 2018/8/26
 * @
 */
public class LocalRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(LocalRunner.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        setArgs(conf, args);
        Job job = Job.getInstance(conf, "LocalRunner");
        job.setJarByClass(LocalRunner.class);
        TableMapReduceUtil.initTableMapperJob(buildList(job),
                LocalMapper.class,
                StatsLocationDimension.class,
                LocalMapOutputValue.class,
                job, true);
        job.setReducerClass(LocalReducer.class);
        job.setOutputKeyClass(StatsLocationDimension.class);
        job.setOutputKeyClass(LocalReduceOutputValue.class);
        job.setOutputFormatClass(IOutputWriterFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private List<Scan> buildList(Job job) {
        Configuration conf = job.getConfiguration();
        long startDate = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE));
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));

        FilterList fl = new FilterList();

        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_UUID,
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_SESSION_ID,
                EventLogConstants.EVENT_COLUMN_NAME_COUNTRY,
                EventLogConstants.EVENT_COLUMN_NAME_PROVINCE,
                EventLogConstants.EVENT_COLUMN_NAME_CITY
        };
        fl.addFilter(this.getColumnFilter(columns));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        scan.setFilter(fl);
        return Lists.newArrayList(scan); // google中的一个包
    }

    /*
     * 此处是使用多多行过滤器，也可以使用单行过滤器，一个一个的设置
     * */
    private Filter getColumnFilter(String[] columns) {
        // 多列簇过滤器
        int length = columns.length;
        byte[][] bytes = new byte[length][];
        for (int i = 0; i < length; i++) {
            bytes[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(bytes);
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.addResource("output-mapping.xml");
        configuration.addResource("writer-mapping.xml");
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        this.setConf(this.conf);
        return this.conf;
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

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LocalRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("执行localRunner失败");
        }
    }
}
