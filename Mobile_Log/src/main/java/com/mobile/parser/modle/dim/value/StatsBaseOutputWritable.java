package com.mobile.parser.modle.dim.value;

import com.mobile.common.KpiTypeEnum;
import org.apache.hadoop.io.Writable;

/**
 *
 *输出value类型的顶级父类
 */
public abstract class StatsBaseOutputWritable implements Writable {
    public abstract KpiTypeEnum getKpi();   //获取kpi的类型


}
