package com.sixth.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:48 2018/8/23
 * @
 */
public class MemberUtil {

    public static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        // 当map的数量到一定程度，就移除map中的数据
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            // 多少M
            return this.size() > 5000;
        }
    };

    /*
     * 判断是否为新增会员
     * true:为新会员
     * */
    public static boolean isNewMember(Connection conn, String memberId) {
        // 去member_info表查询，如果有就是老会员。做一个缓存，如果一个用户登录好多次，总不能每一次都访问数据库吧
        // 先查询缓存，如果没有去member_info查询
        Boolean res = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            res = null;
//            if (cache.containsKey(memberId)) {
            res = cache.get(memberId); // null就需要查询数据库
            if (res == null) {
                ps = conn.prepareStatement("select `member_id` from member_info where `member_id` = ?");
                ps.setString(1, memberId);
                rs = ps.executeQuery();
                if (rs.next()) {
                    // 表示数据库中已经存在过该会员id，即为老会员
                    res = Boolean.valueOf(false);
                } else {
                    // 表示数据库中没有该会员，即是新会员
                    res = Boolean.valueOf(true);
                }
                // 将结果存储到cache中
                cache.put(memberId, res);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, rs);
        }

        return res == null ? false : res.booleanValue();
    }

    /*
     * 删除指定日期的新增用户数据
     * */
    public static void deleteMemberInfoByDate(String date, Connection conn) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("delete from `member_info` where `created` = ?");
            ps.setString(1, date);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, null);
        }
    }
}
