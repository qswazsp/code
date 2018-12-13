package com.mobile.parser.mr.au;

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

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ActiveUserWriter implements ReduceOutputFormat {


    @Override
    public void outputWriter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement pre, DimensionOperateI operateI) {
        StatusUserDimension k = (StatusUserDimension) key;
        ReduceOutputWritable v = (ReduceOutputWritable) value;
        String kpiName = v.getKpi().kipName;
        int activeUser = ((IntWritable) v.getValue().get(new IntWritable(-3))).get();

        int i = 0;

        try {
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getPlatformDimension()));
            if (kpiName.equals(KpiTypeEnum.ACTIVE_USER_BROWSER.kipName)) {
                pre.setInt(++i, operateI.getDimensionIdByDimension(k.getBroswerDimension()));
            }
            pre.setInt(++i, activeUser);
            pre.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            pre.setInt(++i, activeUser);

            //添加批处理中
            pre.addBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
