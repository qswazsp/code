package com.mobile.parser.modle.dim.base;

import com.mobile.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.PlatformName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * 平台的基础维度类
 */
public class PlatformDimension extends BaseDimension {
    private int id;
    private String platformName;

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this(platformName);
        this.id = id;
    }

    public PlatformDimension() {
    }


    public static List<PlatformDimension> buildList(String platformName) {
        if (StringUtils.isBlank(platformName)) {
            platformName = GlobalConstants.RUNNING_DATE;
        }
        List<PlatformDimension> list = new ArrayList<>();
        list.add(new PlatformDimension(platformName));
        return list;
    }

    @Override

    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id - other.id;
        if (tmp > 0) {
            return tmp;
        }
        return this.platformName.compareTo(other.platformName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.platformName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

}
