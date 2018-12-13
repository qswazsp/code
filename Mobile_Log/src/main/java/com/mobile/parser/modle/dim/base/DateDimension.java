package com.mobile.parser.modle.dim.base;

import com.mobile.common.DateType;
import com.mobile.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * 基础时间维度类
 */
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type;

    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar) {
        this(year, season, month, week, day);
        this.calendar = calendar;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar, String type) {
        this(year, season, month, week, day, calendar);
        this.type = type;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar, String type, int id) {
        this(year, season, month, week, day, calendar, type);
        this.id = id;
    }

    public DateDimension() {
    }

    /**
     * 获取统计值的统计周期的时间维度
     * @param time  传入的类型是时间戳类型
     * @param type
     * @return 返回时间维度对象
     */
    public static DateDimension buildDate(long time, DateType type){
        Calendar calendar = Calendar.getInstance();
        calendar.clear();       //清空
        //获取年   精确到年
        int year =TimeUtil.getDateInfo(time ,DateType.YEAR);
        if (type .equals(DateType.YEAR)) {
            calendar.set(year , 0 , 1);
            return new DateDimension(year, 1, 1, 1, 1, calendar.getTime(), type.type);
        }
        //获取季度 精确到当前季度第一个月的1号
        int season =TimeUtil.getDateInfo(time ,DateType.SEASON);
        if (type .equals(DateType.SEASON)) {
            int month = season * 3 - 2;
            calendar.set(year , month - 1 , 1);
            return new DateDimension(year, season, month, 1, 1, calendar.getTime(), type.type);
        }
        //获取月   精确到当月的1号
        int month =TimeUtil.getDateInfo(time ,DateType.MONTH);
        if (type .equals(DateType.MONTH)) {
            calendar.set(year , month - 1 , 1);
            return new DateDimension(year, season, month, 1, 1, calendar.getTime(), type.type);
        }
        //获取周   精确到当周的第一天
        int week =TimeUtil.getDateInfo(time ,DateType.WEEK);
        if (type .equals(DateType.WEEK)) {
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateType.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateType.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateType.MONTH);
            calendar.set(year , month - 1 , 1);
            return new DateDimension(year, season, month, week, 1, calendar.getTime(), type.type);
        }
        //获取月   精确到前一天
        int day =TimeUtil.getDateInfo(time ,DateType.DAY);
        if (type .equals(DateType.DAY)) {
            calendar.set(year , month - 1 , 1);
            return new DateDimension(year, season, month, week, day, calendar.getTime(), type.type);
        }
        throw new RuntimeException("该DateType暂时不支持获取对应的dateDimension" + type.getClass());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        DateDimension other = (DateDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
         tmp = this.year - other.year;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.season - other.season;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.month - other.month;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.week - other.week;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.day - other.day;
        if (tmp != 0) {
            return tmp;
        }
        return this.type.compareTo(other.type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.season);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.week);
        dataOutput.writeInt(this.day);
        dataOutput.writeLong(this.calendar.getTime());//date类型的写为Long类型
        dataOutput.writeUTF(this.type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.season = dataInput.readInt();
        this.month = dataInput.readInt();
        this.week = dataInput.readInt();
        this.day = dataInput.readInt();
        this.calendar.setTime(dataInput.readLong());        //date类型反序列化为
        this.type = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, year, season, month, week, day, calendar, type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
