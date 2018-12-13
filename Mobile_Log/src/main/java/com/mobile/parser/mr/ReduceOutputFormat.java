package com.mobile.parser.mr;

import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.service.DimensionOperateI;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * 自定义为每一个输出sql赋值接口
 */
public interface ReduceOutputFormat {
    /**
     * 将reduce输出的kyy-value值赋值到对应的sql中并添加到批处理中
     * @param conf
     * @param key
     * @param value
     * @param pre
     * @param operateI
     */
    void outputWriter(Configuration conf, BaseDimension key , StatsBaseOutputWritable value,
    PreparedStatement pre , DimensionOperateI operateI);
}
