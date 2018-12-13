package com.mobile.parser.modle.dim.value.map;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyMaperOutput extends StatsBaseOutputWritable {
    private TimeOutputWritable timeOutputWritable = new TimeOutputWritable();
    private String usd;
    private String url;

    public MyMaperOutput() {
    }

    public MyMaperOutput(TimeOutputWritable timeOutputWritable, String uuid, String url) {
        this.timeOutputWritable = timeOutputWritable;
        this.usd = usd;
        this.url = url;
    }

    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        timeOutputWritable.write(out);
        out.writeUTF(usd);
        out.writeUTF(url);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timeOutputWritable.readFields(in);
        this.usd = in.readUTF();
        this.url = in.readUTF();
    }

    public TimeOutputWritable getTimeOutputWritable() {
        return timeOutputWritable;
    }

    public void setTimeOutputWritable(TimeOutputWritable timeOutputWritable) {
        this.timeOutputWritable = timeOutputWritable;
    }

    public String getUsd() {
        return usd;
    }

    public void setUsd(String usd) {
        this.usd = usd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
