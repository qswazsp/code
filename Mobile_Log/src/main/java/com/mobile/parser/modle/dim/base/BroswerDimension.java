package com.mobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 浏览器维度类
 */
public class BroswerDimension extends BaseDimension {
    private int id;
    private String broswerName;
    private String broswerVersion;

    public BroswerDimension() {

    }

    public BroswerDimension(String broswerName, String broswerVersion) {
        this.broswerName = broswerName;
        this.broswerVersion = broswerVersion;
    }

    public BroswerDimension(int id, String broswerName, String broswerVersion) {
        this(broswerName, broswerVersion);
        this.id = id;
    }


    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }

        BroswerDimension other = (BroswerDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.broswerName.compareTo(broswerName);
        if (tmp != 0) {
            return tmp;
        }

        return this.broswerVersion.compareTo(broswerVersion);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.broswerName);
        dataOutput.writeUTF(this.broswerVersion);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.broswerName = dataInput.readUTF();
        this.broswerVersion = dataInput.readUTF();
    }





    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBroswerName() {
        return broswerName;
    }

    public void setBroswerName(String broswerName) {
        this.broswerName = broswerName;
    }

    public String getBroswerVersion() {
        return broswerVersion;
    }

    public void setBroswerVersion(String broswerVersion) {
        this.broswerVersion = broswerVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BroswerDimension that = (BroswerDimension) o;
        return id == that.id &&
                Objects.equals(broswerName, that.broswerName) &&
                Objects.equals(broswerVersion, that.broswerVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, broswerName, broswerVersion);
    }
}
