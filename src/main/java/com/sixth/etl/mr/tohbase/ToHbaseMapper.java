package com.sixth.etl.mr.tohbase;

import com.sixth.common.EventLogConstants;
import com.sixth.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.zip.CRC32;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:44 2018/8/17
 * @ 将hdfs中收集的数据清洗后存储到hbase中
 */
public class ToHbaseMapper extends Mapper<Object, Text, NullWritable, Put> {
    private static final Logger LOGGER = Logger.getLogger(ToHbaseMapper.class);
    // 输入输出的记录
    private int inputRecords, outputRecords, filterRecords = 0;
    private final byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    // 校验内容是否完整，完整性校验
    private CRC32 crc = new CRC32();

    @Override
    protected void map(Object key, Text value, Context context) {
        inputRecords++;
        String log = value.toString();

        // 如果输入进来的内容是null或者长度为0，那么就没有必要解析了
        if (StringUtils.isEmpty(log.trim())) {
            this.filterRecords++;
            return;
        }
        // 正常调用日志工具方法解析log
        Map<String, String> info = LogUtil.handleLog(log);
        // 根据事件来存储数据
        String eventName = info.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME);
        EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventName);
        switch (event) {
            case LAUNCH:
            case PAGEVIEW:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
            case CHARGEREFUND:
            case EVENT:
                handleLogToHbase(info, eventName, context);
                break;
            default:
                LOGGER.warn("事件类型暂时不支持数据的清洗. eventName:" + eventName);
                this.filterRecords++;
                break;
        }
    }

    /*
     * 将每一行数据写出
     * */
    private void handleLogToHbase(Map<String, String> info, String eventName, Context context) {
        if (!info.isEmpty()) {
            // 获取构造row-key的字段
            String serverTime = info.get(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME);
            String uuid = info.get(EventLogConstants.EVENT_COLUMN_NAME_UUID);
            String umid = info.get(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID);

            try {
                if (StringUtils.isNotEmpty(serverTime)) {
                    // TODO 构建row-key
                    String rowKey = buildRowKey(serverTime, uuid, umid, eventName);
                    // 获取hbase的put对象
                    Put put = new Put(Bytes.toBytes(rowKey));
                    // 寻黄info，将所有的k-v存储到row-key行中
                    for (Map.Entry<String, String> entry : info.entrySet()) {
                        // 将kv添加到put中
                        if (StringUtils.isNotEmpty(entry.getKey())) {
                            put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                        }
                    }
                    context.write(NullWritable.get(), put);
                    outputRecords++;
                } else {
                    this.filterRecords++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * 构建row-key
     * */
    private String buildRowKey(String serverTime, String uuid, String umid, String eventName) {
        StringBuffer sb = new StringBuffer(serverTime + "_");
        if (StringUtils.isNotEmpty(serverTime)) {
            // 对crc32的值初始化
            this.crc.reset();
            if (StringUtils.isNotEmpty(uuid)) {
                this.crc.update(uuid.getBytes());
            }
            if (StringUtils.isNotEmpty(umid)) {
                this.crc.update(umid.getBytes());
            }
            if (StringUtils.isNotEmpty(eventName)) {
                this.crc.update(eventName.getBytes());
            }
            // 压缩长度和减少重复的可能性
            sb.append(this.crc.getValue() % 1000000000L);
        }
        return sb.toString();
    }

    // 在所有的map reduce方法执行完了以后执行一次
    @Override
    protected void cleanup(Context context) {
        LOGGER.info("++++inputRecords: " + inputRecords + "  filterRecords: " + filterRecords + "  outputRecords: " + outputRecords + "++++");
    }
}
