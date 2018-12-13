package com.mobile.parser.modle.dim.value.map;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * map阶段输出value类型(用户模块、浏览器模块)
 */
public class TimeOutputWritable extends StatsBaseOutputWritable {
    private String id;      //泛指uid / mid / sid
    private long time;  //传递时间戳 用于统计session的时长

    //map端输出value中没有必要设置kpi,因为在key 中有Kpi的设置
    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();
    }
    //map端输出value中没有必要设置kpi，因为在Kye中有kpi的设置
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }


}
