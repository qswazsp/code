package com.mobile.parser.modle.dim;


import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.base.BroswerDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 用户模块和浏览器模块的map和reduce阶段的输出的key 类型的封装
 */
public class StatusUserDimension extends StatusBasedDimension {

    private StatusCommonDimension statusCommonDimension = new StatusCommonDimension();
    private BroswerDimension broswerDimension = new BroswerDimension();

    public StatusUserDimension(StatusCommonDimension statusCommonDimension, BroswerDimension broswerDimension) {
        this.statusCommonDimension = statusCommonDimension;
        this.broswerDimension = broswerDimension;
    }

    public StatusUserDimension() {
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatusUserDimension other = (StatusUserDimension)o;
        int tmp = this.statusCommonDimension.compareTo(other.statusCommonDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.broswerDimension.compareTo(other.broswerDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statusCommonDimension.write(dataOutput);
        this.broswerDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statusCommonDimension.readFields(dataInput);
        this.broswerDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusUserDimension that = (StatusUserDimension) o;
        return Objects.equals(statusCommonDimension, that.statusCommonDimension) &&
                Objects.equals(broswerDimension, that.broswerDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(statusCommonDimension, broswerDimension);
    }

    public StatusCommonDimension getStatusCommonDimension() {
        return statusCommonDimension;
    }

    public void setStatusCommonDimension(StatusCommonDimension statusCommonDimension) {
        this.statusCommonDimension = statusCommonDimension;
    }

    public BroswerDimension getBroswerDimension() {
        return broswerDimension;
    }

    public void setBroswerDimension(BroswerDimension broswerDimension) {
        this.broswerDimension = broswerDimension;
    }
}
