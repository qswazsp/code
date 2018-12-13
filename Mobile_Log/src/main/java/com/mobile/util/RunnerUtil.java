package com.mobile.util;

import com.mobile.common.DateType;
import com.mobile.common.GlobalConstants;
import com.mobile.parser.modle.dim.base.DateDimension;
import com.mobile.parser.service.DimensionOperateI;
import com.mobile.parser.service.DimensionOperateImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RunnerUtil {

    private void computeTotalUser(Configuration conf , Job job , Logger logger) {
        String todayTime = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        long nowTime = TimeUtil.string2Long(todayTime);
        long yesterTime = nowTime - GlobalConstants.DAY_OF_MILLISECOND;
        //获取维度
        DateDimension todayDimension = DateDimension.buildDate(nowTime, DateType.DAY);
        DateDimension yesterDimension = DateDimension.buildDate(yesterTime, DateType.DAY);
        //获取维度id
        DimensionOperateI operateI = new DimensionOperateImpl();
        int todayDimensionId = -1;
        int yesterDimensionId = -1;

        todayDimensionId = operateI.getDimensionIdByDimension(todayDimension);
        yesterDimensionId = operateI.getDimensionIdByDimension(yesterDimension);
        Map<String, Integer> info = new ConcurrentHashMap<>();

        //查询昨天新增总用户
        //获取数据库连接
        Connection conn = null;
        PreparedStatement pre = null;
        ResultSet rs = null;

        conn = DBUtil.getConn();
        try {
            if (yesterDimensionId > 0) {
                pre = conn.prepareStatement("select platform_dimension_id , total_install_users from stats_user where date_dimension_id = ?");
                pre.setInt(1, yesterDimensionId);
                rs = pre.executeQuery();
                while (rs.next()) {
                    info.put(rs.getInt("platform_dimension_id") + "", rs.getInt("total_install_users"));
                }
            }
            //查询今天的数据
            if (todayDimensionId > 0) {
                pre = conn.prepareStatement("select platform_dimension_id , new_install_users from stats_user where date_dimension_id = ?");
                pre.setInt(1, todayDimensionId);
                rs = pre.executeQuery();
                while (rs.next()) {
                    //遍历每一个platform加到totalInstallUser中
                    int platformID = rs.getInt("platform_dimension_id");
                    int totalInstallUser = rs.getInt("new_install_users");
                    if (info.containsKey(platformID + "")) {
                        totalInstallUser += info.get(platformID + "");
                    }
                    info.put(platformID + "", totalInstallUser);
                }
            }

            for (Map.Entry<String, Integer> en : info.entrySet()) {
                pre = conn.prepareStatement("insert into stats_user (date_dimension_id,platform_dimension_id,total_install_users," +
                        "created) VALUES (?,?,?,?) on duplicate key update total_install_users=?");
                pre.setInt(1, todayDimensionId);
                pre.setInt(2, Integer.parseInt(en.getKey()));
                pre.setInt(3, en.getValue());
                pre.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                pre.setInt(5, en.getValue());
                pre.execute();
            }
        } catch (SQLException e) {
            logger.warn("新增用户计算异常", e);
        } finally {
            DBUtil.close(conn, pre, rs);

        }


    }
}
