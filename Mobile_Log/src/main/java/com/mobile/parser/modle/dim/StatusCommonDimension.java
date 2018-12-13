package com.mobile.parser.modle.dim;

import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.base.DateDimension;
import com.mobile.parser.modle.dim.base.KpiDimension;
import com.mobile.parser.modle.dim.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 封装公共的维度类
 */
public class StatusCommonDimension extends StatusBasedDimension {

    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatusCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    public StatusCommonDimension() {
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatusCommonDimension other = (StatusCommonDimension)o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.kpiDimension.compareTo(other.kpiDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.dateDimension.write(dataOutput);
        this.platformDimension.write(dataOutput);
        this.kpiDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateDimension.readFields(dataInput);           //对象反序列化要注意顺序
        this.platformDimension.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusCommonDimension that = (StatusCommonDimension) o;
        return Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(platformDimension, that.platformDimension) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dateDimension, platformDimension, kpiDimension);
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
