package com.mobile.parser.mr.location;

import com.mobile.common.GlobalConstants;
import com.mobile.parser.modle.dim.StatusLocationDimension;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.MyReduceOutput;
import com.mobile.parser.mr.ReduceOutputFormat;
import com.mobile.parser.service.DimensionOperateI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LocationWriter implements ReduceOutputFormat {
    @Override
    public void outputWriter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement pre, DimensionOperateI operateI) {
        StatusLocationDimension k = (StatusLocationDimension) key;
        MyReduceOutput v = (MyReduceOutput) value;
        //获取
        MapWritable map = v.getReduceOutputWritable().getValue();
        int usdCount = 0;
        int bounceCount = 0;
        for (MapWritable.Entry to : map.entrySet()) {
            usdCount = ((IntWritable)to.getKey()).get();
            bounceCount = ((IntWritable) to.getValue()).get();
        }

        int i = 0;

        try {
            //第一二个参数
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getPlatformDimension()));

            //第三个参数
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getLocationDimension()));
            pre.setInt(++i, v.getUuidCount());
            pre.setInt(++i, usdCount);
            pre.setInt(++i, bounceCount);

            //第四个参数 create
            pre.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));

            //最后一个参数
            pre.setInt(++i, operateI.getDimensionIdByDimension(k.getStatusCommonDimension().getDateDimension()));

            //添加批处理中
            pre.addBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
