package com.sixth.analysic.mr.nu;

import com.google.common.collect.Lists;
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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:55 2018/8/21
 * @ 新增用户的驱动类
 * 设置job，至少设置(考虑)四方面
 * 输入的来源、map阶段、reduce阶段、数据的输出
 */
public class NewUserRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();
    @Override
    public int run(String[] args) throws Exception {
        conf = this.getConf();
        conf.addResource("output-mapping.xml");
        conf.addResource("writer-mapping.xml");
        conf.addResource("total-mapping.xml");

        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "new_user");
        job.setJarByClass(NewUserRunner.class);


        /*
         * 这里请注意：
         * 如果是从hbase输入数据，则使用TableMapReduceUtil工具类来设置mapper的输入
         * 如果是从file中输入数据，则使用FileInputFormat来设置mapper的输入
         * 如果是从其他乱七八糟的地方输入数据，则使用job.setInputFormatClass()来设置---->个人理解，尚未确认
         * */
//        job.setInputFormatClass();

        // 设置mapper相关属性
        // 自己的类是继承的TableMapper
        // addDependency ：true本地提交集群运行
        // 第一个参数指定访问hbase中的哪个表的哪个row-key的哪些个列簇的列
        // 第二个参数指定mapper的类
        // 第三、四个参数指定map阶段输出的key和value的类型
        // 第五个参数指定job
        // 第六个参数指定是否添加依赖，如果为false，表示不添加，即本地提交本地运行；如果为true表示添加，即本地提交集群运行
        TableMapReduceUtil.initTableMapperJob(
                this.buildList(job),
                NewUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job, true);

        /*
         * 这里请注意：
         * 如果是向hbase中输出数据，则使用TableMapReduceUtil来设置reducer的输入
         * 如果是向file中输出数据，则使用FileOutputFormat来设置reducer输入
         * 如果是向mysql数据库中输出数据，则要自定义一个输出类，并使用job.setOutputFormatClass()来设置
         * */
        // 设置reduce类
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutputValue.class);

        // 设置输出的类
        job.setOutputFormatClass(IOutputWriterFormat.class);
//        if (job.waitForCompletion(true)) {
//            this.computeTotalNewUser(job);
//        }
//        return job.waitForCompletion(true) ? 0 : 1;
        if (job.waitForCompletion(true)) {
            this.computeTotalNewUser(job);
            return 0;
        } else {
            return 1;
        }
    }

    /*
     * 新增用户：launch事件去重
     * 新增总用户：当天的新增用户，加上前一天的新增用户
     * 总用户：当天的新增用户，加上前一天的总用户
     *
     * 计算新增总用户，
     * 获取运行当天的新增用户，再获取运行日期前一天的新增总用户，然后相加
     * */
    private void computeTotalNewUser(Job job) {
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
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_total_user"));
                ps.setInt(1, yesterdayDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("total_install_users");
                    // 存储
                    map.put(platformId + "", totalUser);
                }
            }
            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_user"));
                ps.setInt(1, nowDimensionId);

                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUsers = rs.getInt("new_install_users");
                    // 存储
                    if (map.containsKey(platformId + "")) {
                        newUsers += map.get(platformId + "");
                    }
                    map.put(platformId + "", newUsers);
                }
            }
            // 将map中的数据进行更新
            ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX+"new_update_member"));
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                ps.setInt(1, nowDimensionId);
                ps.setInt(2, Integer.parseInt(entry.getKey()));
                ps.setInt(3, entry.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, entry.getValue());
                ps.addBatch();
            }
            // 批量执行
            ps.executeBatch();

        } catch (Exception e) {
            LOGGER.warn("计算新增总用户失败", e);
        } finally {
            JdbcUtil.close(conn, ps, rs);
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
        // 首先往过滤器链中添加一个单行过滤器，这个过滤器的作用是指定某个列满足的条件
        // 即将满足指定条件的所有的数据(所有的列)过滤出来
        fl.addFilter(new SingleColumnValueFilter(
                // 指定哪个列簇
                Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY),
                // 哪个列
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME),
                // 对列值进行过滤的方式
                CompareFilter.CompareOp.EQUAL,
                // 过滤的条件
                Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.alias)));

        // 扫描哪些字段？
        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_UUID,
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

        /*
         * 因为是做日指标，所以我们需要的是传入的日期参数的day
         * 然后将day以设置key/value的方式放入conf
         * */
        // 将时间存储到conf中，跨作业不会丢失值
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }

    @Override
    public void setConf(Configuration configuration) {
//        configuration.addResource("/output-mapping.xml");
//        configuration.addResource("/writer-mapping.xml");
//        configuration.addResource("/total-mapping.xml");
        // 注意这里的方式
//        this.conf = HBaseConfiguration.create(configuration);
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        setConf(conf);
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("新增用户运行失败", e);
        }
    }
}
/*
 * 数据流程：
 * 在Runner中设置map端的输入，从hbase中取出数据
 *     调用TableMapReduceUtil工具类，来设置map端的输入，指定scan
 * scan到的一条数据执行一个map，在map中，定义StatsUserDimension对象作为key，TextOutputValue作为value
 *     定义多个基础的维度类，然后封装到一个维度类中，这样，我们就可以根据不同的维度来统计不同需求的数据
 * 数据输入到reduce，reduce的主要作用就是将相同key的uuid进行去重，去重后的size就是新增用户
 *     新增的用户同样是根据我们指定的维度类查询出来的，所以还是要将维度类作为reduce输出的key
 * 然后就到了数据向mysql中存储
 *     首先我们要拿到各个维度类的id，因为我们存储新增用户数据，要知道是根据什么样的维度条件查询出来的，
 *     根据维度id可以查到维度的所有信息，也就是进行表join
 *     当我们查询维度的时候，如果查询不到，就向对应的维度表中插入这个维度对象信息，然后将id返回
 * 拿到维度的id以后，准备ps
 *     在实现了OutputFormat的类中，有一个getRecordWriter方法，这个方法是返回一个继承了RecordWrite类的对象，
 *     这个对象中的write方法是核心，在write方法中准备ps，并对ps中设置参数，然后执行，这样就将数据写入到mysql中了
 * */