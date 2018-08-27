package com.sixth.analysic.mr.am;

import com.google.common.collect.Lists;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
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
 * @ Date   ：Created in 14:55 2018/8/21
 * @ 活跃用户的驱动类
 * 设置job，至少设置(考虑)四方面
 * 输入的来源、map阶段、reduce阶段、数据的输出
 */
public class ActiveMemberRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(ActiveMemberRunner.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.addResource("writer-mapping.xml");
        conf.addResource("output-mapping.xml");
        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "active_member");
        job.setJarByClass(ActiveMemberRunner.class);

        TableMapReduceUtil.initTableMapperJob(
                this.buildList(job),
                ActiveMemberMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job, true);

        // 设置reduce类
        job.setReducerClass(ActiveMemberReducer.class);
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
        // 设置开始row-key和结束row-key，当然不一定是和row-key一样的格式，只需要记住row-key是字典排序即可
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));
        // 过滤，定义一个过滤器链
        FilterList fl = new FilterList();

        // 扫描哪些字段？
        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID,
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION
        };
        // 将扫描的字段添加到过滤器中，也就是从列簇中的众多列里筛选出我们想要的那几个字段
        fl.addFilter(this.getColumnFilter(columns));
        // 设置hbase表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        // 将过滤器链添加到scan中
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
        // MultipleColumnPrefixFilter是用来指定我们从hbase表中将哪些列查询出来
        return new MultipleColumnPrefixFilter(bytes);
    }

    private void setArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                date = args[i + 1];
            }
        }
        // 如果时间为空或者非法，都将运行昨天的数据
        if (StringUtils.isEmpty(date) || !TimeUtil.isValidateDate(date)) {
            date = TimeUtil.getYesterday();
        }
        // 将时间存储到conf中，跨作业不会丢失值
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
            ToolRunner.run(new Configuration(), new ActiveMemberRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("活跃用户运行失败", e);
        }
    }
}
