package com.mobile.parser.modle.dim;

import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatusLocationDimension extends BaseDimension {
    private StatusCommonDimension statusCommonDimension = new StatusCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatusLocationDimension() {
    }

    public StatusLocationDimension(StatusCommonDimension statusCommonDimension, LocationDimension locationDimension) {
        this.statusCommonDimension = statusCommonDimension;
        this.locationDimension = locationDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatusLocationDimension other = (StatusLocationDimension) o;
        int tmp = this.statusCommonDimension.compareTo(other.statusCommonDimension);
        if (tmp != 0) {
            return tmp;
        }
        return locationDimension.compareTo(other.locationDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        statusCommonDimension.write(out);
        locationDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        statusCommonDimension.readFields(in);
        locationDimension.readFields(in);
    }

    public StatusCommonDimension getStatusCommonDimension() {
        return statusCommonDimension;
    }

    public void setStatusCommonDimension(StatusCommonDimension statusCommonDimension) {
        this.statusCommonDimension = statusCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusLocationDimension that = (StatusLocationDimension) o;
        return Objects.equals(statusCommonDimension, that.statusCommonDimension) &&
                Objects.equals(locationDimension, that.locationDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCommonDimension, locationDimension);
    }
}
