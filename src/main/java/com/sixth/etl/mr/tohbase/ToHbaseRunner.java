package com.sixth.etl.mr.tohbase;

import com.sixth.common.EventLogConstants;
import com.sixth.common.GlobalConstants;
import com.sixth.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:38 2018/8/17
 * @
 */
public class ToHbaseRunner implements Tool {
    private static final Logger LOGGER = Logger.getLogger(ToHbaseRunner.class);
    private Configuration conf = null;

    @Override
    public int run(String[] args) throws Exception {
        conf = this.getConf();
        // 处理参数，得到路径，并将路径放入conf
        processArgs(args, conf);
        Job job = Job.getInstance(conf, "to hbase");

        job.setJarByClass(ToHbaseRunner.class);

        // map阶段
        job.setMapperClass(ToHbaseMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        // 判断hbase的表是否存在，不存在则创建
        // 方法内部：当判断表不存在就创建表
        isTableExists(job);

        // 本地提交本地运行：false，将jar包做成一个临时的jar包
        // 也有本地提交集群运行：true，默认是true
        // 如果要放到集群上运行，需要将false设置为true
//        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_TABLE_NAME, null, job);
        // TableMapReduceUtil是专门用来处理hbase的mapreduce的工具类
        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_TABLE_NAME,
                null, job, null, null, null, null, false);

        // reducer端的设置
        // 因为不需要reduce，所以直接将reduceTask设置为0
        // 上面的map端输出的设置，可以直接设置成reduce端输出的设置----自己猜测(因为没有reduce端，所以map端的输出就是reduce端的输出)
        job.setNumReduceTasks(0);
        // 将不能识别的资源文件添加分布式的缓存文件，qqwry.dat
        // 1、通过job将文件缓存，新版的API可以不再设置符号链接了
        // 设置符号链接就是在路径的最后面加上一个#symlink，在mapper类中通过context.getSymlink()可以判断是否开启了创建符号链接
//        job.addCacheFile(new URI());
        // 2、在map阶段可以通过context拿到缓存数据，一般是在初始化的时候使用，也就是setUpf方法
//        context.getCacheFiles()和context.getLocalCacheFile()方法

        // 将文件加载到分布式缓存
//        DistributedCache.addCacheFile(new URI(""), conf);
//        DistributedCache.getLocalCacheFiles(conf);
        // 从分布式缓存中加载文件
//        URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
//        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);

        // 设置输入，FileInputFormat
        setInputPath(job, args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 处理参数
     * */
    private void processArgs(String[] args, Configuration conf) {
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

    /*
     * 设置清洗数据的输入路径
     * */
    private void setInputPath(Job job, String[] args) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        // 拆分日期，日期格式：2018-08-17等
        String[] fields = date.split("-");
        Path inputPath = new Path("/logs/js/" + fields[1] + "/" + fields[2]);
        try {

            FileSystem fs = FileSystem.get(job.getConfiguration());
            if (fs.exists(inputPath)) {
                // 将输入路径注入job中
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("数据输入路径不存在.inputPath: " + inputPath.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 判断hbase表，预分区
     * */
    private void isTableExists(Job job) {
        // 有配置文件可以不用写配置信息
        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection();
            admin = conn.getAdmin();

            TableName tn = TableName.valueOf(EventLogConstants.HBASE_TABLE_NAME);
            if (!admin.tableExists(tn)) { // 判断表是否存在
                // namespace是调用create()方法来实例化的，注意区别
                HTableDescriptor htd = new HTableDescriptor(tn);
                HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(
                        EventLogConstants.HBASE_COLUMN_FAMILY));
                htd.addFamily(hcd);

                admin.createTable(htd);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // do nothing
                }
            }

        }
    }

    // 使用HbaseConfiguration类的create方法来实例化一个conf，应该是会自动加载resource中的文件到conf
    // 然后在初始化job的时候将conf注入
    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ToHbaseRunner(), args);
        } catch (Exception e) {
            LOGGER.warn("运行清洗数据到hbase中异常", e);
        }
    }
}