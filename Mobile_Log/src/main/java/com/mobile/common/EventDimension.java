package com.mobile.common;

import com.mobile.parser.modle.dim.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EventDimension extends BaseDimension {
    private int id;
    private String category;
    private String action;

    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    public EventDimension() {
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        EventDimension other = (EventDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.category.compareTo(other.category);
        if (tmp != 0) {
            return tmp;
        }
        return this.action.compareTo(other.action);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(category);
        out.writeUTF(action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
    }
}
