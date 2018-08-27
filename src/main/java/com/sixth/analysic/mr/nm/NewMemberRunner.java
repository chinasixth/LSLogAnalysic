package com.sixth.analysic.mr.nm;

import com.sixth.analysic.model.dim.base.DateDimension;
import com.sixth.analysic.model.dim.StatsUserDimension;
import com.sixth.analysic.model.dim.value.map.TimeOutputValue;
import com.sixth.analysic.model.dim.value.reduce.TextOutputValue;
import com.sixth.analysic.mr.IOutputWriterFormat;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.DateEnum;
import com.sixth.common.EventLogConstants;
import com.sixth.common.GlobalConstants;
import com.sixth.util.JdbcUtil;
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:55 2018/8/21
 * @ 新增会员的驱动类
 * 设置job，至少设置(考虑)四方面
 * 输入的来源、map阶段、reduce阶段、数据的输出
 */
public class NewMemberRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        conf = this.getConf();
        conf.addResource("writer-mapping.xml");
        conf.addResource("output-mapping.xml");
        conf.addResource("total-mapping.xml");
        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "new_member");
        job.setJarByClass(NewMemberRunner.class);

        TableMapReduceUtil.initTableMapperJob(
                this.buildList(job),
                NewMemberMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job, true);

        // 设置reduce类
        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutputValue.class);

        // 设置输出的类
        job.setOutputFormatClass(IOutputWriterFormat.class);

//        return job.waitForCompletion(true) ? 0 : 1;
        if (job.waitForCompletion(true)) {
            computeTotalNewMemeber(job);
            return 0;
        } else {
            return 1;
        }

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
        List<Scan> list = new ArrayList<>();
        list.add(scan);
        return list;
//        return Lists.newArrayList(scan); // google中的一个包
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

    private void computeTotalNewMemeber(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 获取运行当天的日期
            long nowDate = TimeUtil.parseString2Long(job.getConfiguration().get(GlobalConstants.RUNNING_DATE));
            // 运行当天的前一天日期
            long yesterdayDate = nowDate - GlobalConstants.DAY_OF_MILLISECONDS;
            // 构建时间维度对象
            DateDimension nowDateDimension = DateDimension.buildDate(nowDate, DateEnum.DAY);
            DateDimension yesterdayDimension = DateDimension.buildDate(yesterdayDate, DateEnum.DAY);

            // 获取对应时间维度的id
            int nowDimensionId = -1;
            int yesterdayDimensionId = -1;

            IDimensionConvert convert = new IDimensionConvertImpl();

            nowDimensionId = convert.getDimensionByValue(nowDateDimension);
            yesterdayDimensionId = convert.getDimensionByValue(yesterdayDimension);

            conn = JdbcUtil.getConn();
            Map<String, Integer> map = new HashMap<>();
            // 获取昨天的新增总用户
            if (yesterdayDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_total_member"));
                ps.setInt(1, yesterdayDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("total_members");
                    // 存储
                    map.put(platformId + "", totalUser);
                }
            }
            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_member"));
                ps.setInt(1, nowDimensionId);

                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUsers = rs.getInt("new_members");
                    // 存储
                    if (map.containsKey(platformId + "")) {
                        newUsers += map.get(platformId + "");
                    }
                    map.put(platformId + "", newUsers);
                }
            }
            // 将map中的数据进行更新
            ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_update_member"));
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                ps.setInt(1, nowDimensionId);
                ps.setInt(2, Integer.parseInt(entry.getKey()));
                ps.setInt(3, entry.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, entry.getValue());
//                rs = ps.executeQuery();
                ps.addBatch();
            }
            // 批量执行
            ps.executeBatch();

        } catch (Exception e) {
            LOGGER.warn("计算新增总会员失败", e);
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewMemberRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("新增会员用户运行失败", e);
        }
    }
}
