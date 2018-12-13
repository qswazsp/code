package com.mobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LocationDimension extends BaseDimension {
    private int id ;
    private String country;
    private String province;
    private String city;

    public LocationDimension(String country,  String city) {
        this.country = country;
        this.city = city;
    }

    public LocationDimension(String country, String province, String city) {
        this(country, city);
        this.province = province;
    }

    public LocationDimension() {
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        LocationDimension other = (LocationDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (tmp != 0) {
            return tmp;
        }
        return this.city.compareTo(other.city);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(country);
        out.writeUTF(province);
        out.writeUTF(city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, country, province, city);
    }
}
