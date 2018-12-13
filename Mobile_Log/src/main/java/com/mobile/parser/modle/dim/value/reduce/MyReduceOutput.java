package com.mobile.parser.modle.dim.value.reduce;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyReduceOutput extends StatsBaseOutputWritable {
    private ReduceOutputWritable reduceOutputWritable = new ReduceOutputWritable();
    private int uuidCount ;

    public MyReduceOutput() {
    }

    public MyReduceOutput(ReduceOutputWritable reduceOutputWritable, int uuidCount) {
        this.reduceOutputWritable = reduceOutputWritable;
        this.uuidCount = uuidCount;
    }

    @Override
    public KpiTypeEnum getKpi() {
        return reduceOutputWritable.getKpi();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        reduceOutputWritable.write(out);
        out.writeInt(uuidCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reduceOutputWritable.readFields(in);
        this.uuidCount = in.readInt();
    }

    public ReduceOutputWritable getReduceOutputWritable() {
        return reduceOutputWritable;
    }

    public void setReduceOutputWritable(ReduceOutputWritable reduceOutputWritable) {
        this.reduceOutputWritable = reduceOutputWritable;
    }

    public int getUuidCount() {
        return uuidCount;
    }

    public void setUuidCount(int uuidCount) {
        this.uuidCount = uuidCount;
    }
}
