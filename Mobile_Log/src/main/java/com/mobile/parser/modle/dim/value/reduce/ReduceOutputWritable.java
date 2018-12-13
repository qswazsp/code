package com.mobile.parser.modle.dim.value.reduce;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * reduce端输出的value的封装
 */
public class ReduceOutputWritable extends StatsBaseOutputWritable {
    private MapWritable value = new MapWritable();      //k-v形式
    private KpiTypeEnum kpi;    //reduce端输出后需要kpi标识指标，也需要kpi指定输出的表


    public void write(DataOutput dataOutput) throws IOException {
        this.value.write(dataOutput);
        WritableUtils.writeEnum(dataOutput, this.kpi);      //枚举的序列化
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.value.readFields(dataInput);
        WritableUtils.readEnum(dataInput, KpiTypeEnum.class);       //枚举的反序列化
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiTypeEnum kpi) {
        this.kpi = kpi;
    }

    @Override
    public KpiTypeEnum getKpi() {
        return this.kpi;
    }
}
