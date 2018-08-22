package com.sixth.analysic.mr;

import com.sixth.analysic.model.dim.BaseStatsDimension;
import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;
import com.sixth.common.GlobalConstants;
import com.sixth.common.KpiType;
import com.sixth.etl.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:34 2018/8/21
 * @ 自定义reduce阶段输出格式类
 * reduce端输出的key和value，会来到这里，然后进行最终的输出操作，比如向mysql中写出数据
 */
public class IOutputWriterFormat extends OutputFormat<BaseStatsDimension, BaseOutputValueWritable> {
    private static final Logger LOGGER = Logger.getLogger(IOutputWriterFormat.class);

    /*
     * 核心
     * */
    @Override
    public RecordWriter<BaseStatsDimension, BaseOutputValueWritable> getRecordWriter(
            TaskAttemptContext taskAttemptContext) {
        // 从上下文对象中获取conf，这样在Runner的run方法中对conf进行设置都会传过来，比如对conf加载的一些xml文件
        Configuration conf = taskAttemptContext.getConfiguration();
//        conf.addResource("output-mapping.xml");
//        conf.addResource("writer-mapping.xml");
//        conf.addResource("total-mapping.xml");

        // 因为是往数据库中写，所以要拿到一个数据库的连接
        Connection conn = JdbcUtil.getConn();
        // 我们构建dimension的时候，并没有指定id，所以要调用专门的id转换器
        IDimensionConvert convert = new IDimensionConvertImpl();

        // 将上面准备的参数传入到一个继承了RecordWrite的类中，开始真正的输出
        return new IOutputRecordWriter(conn, conf, convert);
    }

    // 检测输出空间
    @Override
    public void checkOutputSpecs(JobContext jobContext) {
        // do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
            throws IOException {
        // 输出到mysql，所以没有输出路径
        return new FileOutputCommitter(null, taskAttemptContext);
        // 两个都可以
//        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext), taskAttemptContext);
    }

    /*
     * 封装输出记录的内部类
     * */
    public class IOutputRecordWriter extends RecordWriter<BaseStatsDimension, BaseOutputValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;
        // 定义两个集合，用于做缓存
        // kpi累计的sql，当达到一定的条件时，统一进行sql
        private Map<KpiType, Integer> batch = new HashMap<>();
        // kpi对应的ps，要做到批量执行ps
        private Map<KpiType, PreparedStatement> map = new HashMap<>();

        /*
         * 核心方法
         * */
        @Override
        public void write(BaseStatsDimension key, BaseOutputValueWritable value) {
            // 先判断reduce结束输出过来的key和value是否为空，为空返回，不为空开始执行输出
            if (key == null || value == null) {
                return;
            }
            // 获取kpi来获取对应的sql，也就是ps
            // 从value中获取
            try {
                KpiType kpi = value.getKpi();
                PreparedStatement ps = null;
                int counter = 1;
                if (map.containsKey(kpi)) {
                    // map中有kpi，直接获得
                    ps = map.get(kpi);
                    counter = this.batch.get(kpi);
                    counter++;
                } else {
                    // 如果没有kpi
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi, ps);
                }
                this.batch.put(kpi, counter);

                // 为ps赋值
                String writerClassName = conf.get(GlobalConstants.WRITER_PREFIX + kpi.kpiName);
                Class<?> classz = Class.forName(writerClassName);
                // 将类转换成接口对象
                IOutputWriter writer = (IOutputWriter) classz.newInstance();
                // 调用writer方法
                writer.writer(conf, key, value, ps, convert); // 调用对应的赋值类

                // 将赋值好的ps达到批量处理
                if (counter % GlobalConstants.BATCH_NUMBER == 0) {
                    // 批量执行
                    ps.executeBatch();
                    conn.commit(); // 可以不要
                    this.batch.remove(kpi); // 移除已经执行的kpi的ps
                }
            } catch (Exception e) {
                LOGGER.warn("执行写recordWriter的write方法失败", e);
            }
        }

        /*
         * 将最后不足50的ps再批量执行一次
         * */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) {
            try {
                // 循环map并将其中的ps执行
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    en.getValue().executeBatch(); // 将剩余的ps执行
//                    conn.commit();
                }
            } catch (SQLException e) {
                LOGGER.error("在close时，执行剩余的ps错误", e);
            } finally {
                JdbcUtil.close(conn, null, null);
                // 循环将执行完成后的ps移除
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    JdbcUtil.close(null, en.getValue(), null);
                }
            }
        }

        /*
        * 当new IOutputRecordWriter的时候，将传入的参数赋值给本地属性
        * */
        public IOutputRecordWriter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        public IOutputRecordWriter() {
        }
    }
}
