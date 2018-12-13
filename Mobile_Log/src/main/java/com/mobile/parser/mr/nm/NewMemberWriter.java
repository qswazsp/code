package com.mobile.parser.mr.nm;

import com.mobile.common.GlobalConstants;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.ReduceOutputFormat;
import com.mobile.parser.service.DimensionOperateI;
import com.mobile.util.DBUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class NewMemberWriter implements ReduceOutputFormat {
    @Override
    public void outputWriter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement pre, DimensionOperateI operateI) {
        StatusUserDimension k = (StatusUserDimension) key;
        ReduceOutputWritable v = (ReduceOutputWritable) value;
        MapWritable map = v.getValue();
        Iterator<Map.Entry<Writable, Writable>> it = map.entrySet().iterator();
        Connection conn = DBUtil.getConn();
        String sql = "select * from member_info where member_id=?";
        String sql2 = "insert into member_info (member_id, last_visit_date, member_id_server_date, created) values (?,?,?,?) ";
        PreparedStatement ps = null;
        ResultSet rs = null;
        int newCount = 0;          //计算新增总数
        //要想知道uid是否是新用户，需要查询数据库，但由于需要往数据库中插入数据两次，
        // 所以当第一次获取到数据时，将其map值设置为0,以区分与第二次数据的插入
        try {
            ps = conn.prepareStatement(sql);
            while (it.hasNext()) {
                Map.Entry<Writable, Writable> entry = it.next();
                int memberID = ((IntWritable) entry.getKey()).get();
                ps.setInt(1, memberID);
                rs = ps.executeQuery();
                int mapValue = ((IntWritable) entry.getValue()).get();
                if (!rs.next()) {
                    //第一次取到数据先查询，如果没有则插入到数据库中，并把value值改为0
                    if (mapValue == -1) {
                        //插入数据操作
                        ps = conn.prepareStatement(sql2);
                        ps.setInt(1, memberID);
                        ps.setString(2, conf.get(GlobalConstants.RUNNING_DATE));
                        ps.setInt(3, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
                        ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                        ps.execute();
                        entry.setValue(new IntWritable(0));
                        newCount++;
                    }
                } else {
                    //判断是否是第一次写入,如果是则将计数加一
                    if (mapValue == 0) {
                        newCount++;
                        entry.setValue(new IntWritable(1));
                    }
                }
            }
            DBUtil.close(conn, ps, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String kpiName = v.getKpi().kipName;

        int i = 0;

        try {
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getPlatformDimension()));
            if (kpiName.equals(KpiTypeEnum.NEW_MEMBER_BROWSER.kipName)) {
                pre.setInt(++i, operateI.getDimensionIdByDimension(k.getBroswerDimension()));
            }
            pre.setInt(++i, newCount);
            pre.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            pre.setInt(++i, newCount);

            //添加批处理中
            pre.addBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
