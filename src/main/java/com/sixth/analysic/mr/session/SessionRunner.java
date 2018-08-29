package com.sixth.analysic.mr.session;

import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
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
 * @ Date   ：Created in 14:55 2018/8/21
 * @ 新增会员的驱动类
 * 设置job，至少设置(考虑)四方面
 * 输入的来源、map阶段、reduce阶段、数据的输出
 */
public class SessionRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(SessionRunner.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        conf = this.getConf();
        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "session_length");
        job.setJarByClass(SessionRunner.class);

        TableMapReduceUtil.initTableMapperJob(
                this.buildList(job),
                SessionMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job, true);

        // 设置reduce类
        job.setReducerClass(SessionReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutputValue.class);

        // 设置输出的类
        job.setOutputFormatClass(IOutputWriterFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 获取hbase的扫描对象
     * */
    private List<Scan> buildList(Job job) {
        Configuration conf = job.getConfiguration();
        long startDate = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE));
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));
        FilterList fl = new FilterList();

        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_SESSION_ID,
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION
        };
        fl.addFilter(this.getColumnFilter(columns));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        scan.setFilter(fl);
        List<Scan> list = new ArrayList<>();
        list.add(scan);
        return list;
    }

    /*
     * 此处是使用多多行过滤器，也可以使用单行过滤器，一个一个的设置
     * */
    private Filter getColumnFilter(String[] columns) {
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
        conf.addResource("writer-mapping.xml");
        conf.addResource("output-mapping.xml");
        conf.addResource("total-mapping.xml");
    }

    @Override
    public Configuration getConf() {
        setConf(this.conf);
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new SessionRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("sessionLength运行失败", e);
        }
    }
}
