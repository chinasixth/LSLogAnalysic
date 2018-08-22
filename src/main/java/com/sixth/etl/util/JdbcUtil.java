package com.sixth.etl.util;

import com.sixth.common.GlobalConstants;

import java.sql.*;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:37 2018/8/21
 * @ 获取和关闭数据库的连接
 */
public class JdbcUtil {

    // 静态加载驱动
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL, GlobalConstants.USERNAME, GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                //
            }
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
    }
}
