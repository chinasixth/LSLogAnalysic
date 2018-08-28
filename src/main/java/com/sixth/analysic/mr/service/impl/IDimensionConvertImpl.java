package com.sixth.analysic.mr.service.impl;

import com.sixth.analysic.model.dim.base.*;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.util.JdbcUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:36 2018/8/21
 * @ 根据维度获取维度id接口实现
 */
public class IDimensionConvertImpl implements IDimensionConvert {
    private static final Logger LOGGER = Logger.getLogger(IDimensionConvertImpl.class);
    // 缓存中最多缓存一千个
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size() > 1000;
        }
    };

    /*
     * 获取对应维度的id
     * */
    @Override
    public int getDimensionByValue(BaseDimension dimension) {
        int id = 0;
        try {
            // 生成维度的缓存key
            String cacheKey = buildCacheKey(dimension);
            if (this.cache.containsKey(cacheKey)) {
                return this.cache.get(cacheKey);
            }
            // 缓存中没有对应的维度
            // 去mysql中线查找，如果有则返回id，没有将先插入再返回对应的维度id
            Connection conn = JdbcUtil.getConn();
            String[] sqls = null;
            if (dimension instanceof PlatformDimension) {
                sqls = buildPlatformSqls(dimension);
            }
            if (dimension instanceof KpiDimension) {
                sqls = buildKpiSqls(dimension);
            }
            if (dimension instanceof BrowserDimension) {
                sqls = buildBrowserSqls(dimension);
            }
            if (dimension instanceof DateDimension) {
                sqls = buildDateSqls(dimension);
            }
            if (dimension instanceof LocationDimension) {
                sqls = buildLocalSqls(dimension);
            }
            if (dimension instanceof EventDimension) {
                sqls = buildEventSqls(dimension);
            }
            if (dimension instanceof CurrencyTypeDimension) {
                sqls = buildCurrencyTypeSqls(dimension);
            }
            if (dimension instanceof PaymentTypeDimension) {
                sqls = buildPaymentTypeSqls(dimension);
            }
            // 执行sql
            id = -1;
            synchronized (this) {
                id = this.executeSqls(sqls, dimension, conn);
            }
            // 将获取的id放入缓存中
            this.cache.put(cacheKey, id);
            return id;
        } catch (Exception e) {
            LOGGER.warn("warn: 获取维度id异常", e);
        }
        throw new RuntimeException("获取维度id异常");
    }

    private int executeSqls(String[] sqls, BaseDimension dimension, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sqls[0]);
            // 为ps赋值
            this.setArgs(dimension, ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            // 没有查询到对应的id
            // 在插入的时候返回生成的主键，也就是key
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            setArgs(dimension, ps);
            ps.executeUpdate();
            // 和上面的配合使用，这样的话，就避免了插入以后还要操作
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            LOGGER.warn("执行sql异常", e);
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        throw new RuntimeException("执行sql语句时运行异常");
    }

    /*
     * 设置参数
     * */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (dimension instanceof PlatformDimension) {
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i, platform.getPlatformName());
            }
            if (dimension instanceof KpiDimension) {
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i, kpi.getKpiName());
            }
            if (dimension instanceof BrowserDimension) {
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i, browser.getBrowserName());
                ps.setString(++i, browser.getBrowserVersion());
            }
            if (dimension instanceof DateDimension) {
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeek());
                ps.setInt(++i, date.getDay());
                ps.setString(++i, date.getType());
                ps.setDate(++i, new Date(date.getCalendar().getTime()));
            }
            if (dimension instanceof LocationDimension) {
                LocationDimension local = (LocationDimension) dimension;
                ps.setString(++i, local.getCountry());
                ps.setString(++i, local.getProvince());
                ps.setString(++i, local.getCity());
            }
            if (dimension instanceof EventDimension) {
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i, event.getCategory());
                ps.setString(++i, event.getAction());
            }
            if (dimension instanceof CurrencyTypeDimension) {
                CurrencyTypeDimension currencyTypeDimension = (CurrencyTypeDimension) dimension;
                ps.setString(++i, currencyTypeDimension.getCurrenctName());
            }
            if (dimension instanceof PaymentTypeDimension) {
                PaymentTypeDimension paymentTypeDimension = (PaymentTypeDimension) dimension;
                ps.setString(++i, paymentTypeDimension.getPaymentType());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String[] buildPaymentTypeSqls(BaseDimension dimension) {
        String query = "select id from `dimension_payment_type` where `payment_type` = ?";
        String insert = "insert into `dimension_payment_type`(`payment_type`) values(?)";
        return new String[]{query, insert};
    }

    private String[] buildCurrencyTypeSqls(BaseDimension dimension) {
        String query = "select id from `dimension_currency_type` where `currency_name` = ?";
        String insert = "insert into `dimension_currency_type`(`currency_name`) values(?)";
        return new String[]{query, insert};
    }
    private String[] buildEventSqls(BaseDimension dimension) {
        String query = "select id from `dimension_event` where `category` = ? and `action` = ?";
        String insert = "insert into `dimension_event`(`category`,`action`) values(?,?)";
        return new String[]{query, insert};
    }

    private String[] buildLocalSqls(BaseDimension dimension) {
        String query = "select id from `dimension_location` where `country` = ? and `province` = ? and  `city` = ?";
        String insert = "insert into `dimension_location`(`country`,`province`,`city`) values(?,?,?)";
        return new String[]{query, insert};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String query = "select id from `dimension_date` where `year` = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `type` = ? and `calendar` = ?";
        String insert = "insert into `dimension_date` (`year`,`season`,`month`,`week`,`day`,`type`, `calendar`) values(?,?,?,?,?,?,?)";
        return new String[]{query, insert};
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String query = "select id from  `dimension_browser` where `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into dimension_browser (`browser_name`,`browser_version`) values(?,?)";
        return new String[]{query, insert};
    }

    private String[] buildKpiSqls(BaseDimension dimension) {
        String query = "select id from `dimension_kpi` where `kpi_name` = ?";
        String insert = "insert into `dimension_kpi` (`kpi_name`) values(?)";
        return new String[]{query, insert};
    }

    /*
     * 第一个查询id
     * 第二个插入
     * */
    private String[] buildPlatformSqls(BaseDimension dimension) {
        String query = "select id from `dimension_platform` where `platform_name` = ?";
        String insert = "insert into `dimension_platform` (`platform_name`) values(?)";
        return new String[]{query, insert};
    }

    /*
     * 构建缓存的key
     * 使用字符串
     * */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();

        if (dimension instanceof PlatformDimension) {
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append("platform_");
            sb.append(platform.getPlatformName());
        }
        if (dimension instanceof KpiDimension) {
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append("kpi_");
            sb.append(kpi.getKpiName());
        }
        if (dimension instanceof BrowserDimension) {
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append("browser_");
            sb.append(browser.getBrowserName());
        }
        if (dimension instanceof DateDimension) {
            DateDimension date = (DateDimension) dimension;
            sb.append("date_");
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        }
        if (dimension instanceof LocationDimension) {
            LocationDimension local = (LocationDimension) dimension;
            sb.append("local_");
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        }
        if (dimension instanceof EventDimension) {
            EventDimension event = (EventDimension) dimension;
            sb.append("event_");
            sb.append(event.getCategory());
            sb.append(event.getAction());
        }
        if (dimension instanceof CurrencyTypeDimension) {
            CurrencyTypeDimension currencyTypeDimension = (CurrencyTypeDimension) dimension;
            sb.append("currencyType_");
            sb.append(currencyTypeDimension.getCurrenctName());
        }
        if (dimension instanceof PaymentTypeDimension) {
            PaymentTypeDimension paymentTypeDimension = (PaymentTypeDimension) dimension;
            sb.append("paymentType_");
            sb.append(paymentTypeDimension.getPaymentType());
        }
        return sb.toString();
    }
}
