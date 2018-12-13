package com.mobile.parser.util;

import com.mobile.common.DateType;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.util.TimeUtil;


public class LoadData {

    public static void loadData(BaseDimension dimension, String[] fields) {
        StatusUserDimension userDimension = (StatusUserDimension) dimension;
        //platName
        userDimension.getStatusCommonDimension().getPlatformDimension().setPlatformName(fields[2]);
        //DateDimension
        long time = Long.parseLong(fields[15]);
        userDimension.getStatusCommonDimension().getDateDimension().setYear(TimeUtil.getDateInfo(time, DateType.YEAR));
        userDimension.getStatusCommonDimension().getDateDimension().setSeason(TimeUtil.getDateInfo(time, DateType.SEASON));
        userDimension.getStatusCommonDimension().getDateDimension().setMonth(TimeUtil.getDateInfo(time, DateType.MONTH));
        userDimension.getStatusCommonDimension().getDateDimension().setWeek(TimeUtil.getDateInfo(time, DateType.WEEK));
        userDimension.getStatusCommonDimension().getDateDimension().setDay(TimeUtil.getDateInfo(time, DateType.DAY));
        //BroswerDimension
        userDimension.getBroswerDimension().setBroswerName(fields[26]);
        userDimension.getBroswerDimension().setBroswerVersion(fields[27]);

    }
}
