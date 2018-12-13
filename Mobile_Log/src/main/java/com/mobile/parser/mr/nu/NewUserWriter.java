package com.mobile.parser.mr.nu;

import com.mobile.common.GlobalConstants;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.ReduceOutputFormat;
import com.mobile.parser.service.DimensionOperateI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NewUserWriter implements ReduceOutputFormat {
    @Override
    public void outputWriter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement pre, DimensionOperateI operateI) {
        StatusUserDimension k = (StatusUserDimension) key;
        ReduceOutputWritable v = (ReduceOutputWritable) value;
        String kpiName = v.getKpi().kipName;
        String sessionId = "";
        Long length = 0l;
        int newUsers = 0;
        if (kpiName.equals(KpiTypeEnum.SESSION.kipName) || kpiName.equals(KpiTypeEnum.SESSION_BROWSER.kipName)) {
            for (MapWritable.Entry to : v.getValue().entrySet()) {
                sessionId = to.getKey().toString();
                length = ((LongWritable) to.getValue()).get();
            }
        } else {
            newUsers = ((IntWritable) v.getValue().get(new IntWritable(-1))).get();
        }

        int i = 0;

        try {
            //第一批参数     公共维度 date 和 platform
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getPlatformDimension()));

            //第二批参数     是否有browser
            if (kpiName.equals(KpiTypeEnum.ACTIVE_USER_BROWSER.kipName) || kpiName.equals(KpiTypeEnum.ACTIVE_NUMBER_BROWSER.kipName) || kpiName.equals(KpiTypeEnum.SESSION_BROWSER.kipName)
                    || KpiTypeEnum.PAGEVIEW.kipName.equals(kpiName)) {
                pre.setInt(++i, operateI.getDimensionIdByDimension(k.getBroswerDimension()));
            }

            //第三批参数
            if (kpiName.equals(KpiTypeEnum.SESSION.kipName) || kpiName.equals(KpiTypeEnum.SESSION_BROWSER.kipName)) {
                pre.setString(++i, sessionId);
                pre.setLong(++i, length);
            } else {
            pre.setInt(++i, newUsers);
            }

            //第四个参数 create
            pre.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));

            //最后一个参数
            if (kpiName.equals(KpiTypeEnum.SESSION.kipName) || kpiName.equals(KpiTypeEnum.SESSION_BROWSER.kipName)
                    || kpiName.equals(KpiTypeEnum.PAGEVIEW.kipName)) {
                pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
            } else {
                pre.setInt(++i, newUsers);
            }

            //添加批处理中
            pre.addBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
