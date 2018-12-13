package com.mobile.parser.mr.location;

import com.mobile.common.DateType;
import com.mobile.common.EnumEvent;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusCommonDimension;
import com.mobile.parser.modle.dim.StatusLocationDimension;
import com.mobile.parser.modle.dim.base.DateDimension;
import com.mobile.parser.modle.dim.base.KpiDimension;
import com.mobile.parser.modle.dim.base.LocationDimension;
import com.mobile.parser.modle.dim.base.PlatformDimension;
import com.mobile.parser.modle.dim.value.map.MyMaperOutput;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.mr.au.ActiveUserMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

    //维度是country, province, city    map<usd,pv>    stime
public class LocationMapper extends Mapper<LongWritable, Text, StatusLocationDimension, MyMaperOutput> {
    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private static StatusLocationDimension k = new StatusLocationDimension();
    private static MyMaperOutput v = new MyMaperOutput();

    private KpiDimension pageKpi = new KpiDimension(KpiTypeEnum.PAGEVIEW.kipName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        String en = fields[0];
        String UUID = fields[6];
        String usd = fields[9];
        String url = fields[12];
        String serverTime = fields[15];
        String platform = fields[2];
        String country = fields[30];
        String province = fields[31];
        String city = fields[32];
        //过滤
        if (!en.equals(EnumEvent.PAGEVIEW.alias)) {
            logger.warn("不是pageView事件");
            return;
        }

        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(UUID)) {
            return;
        }

        long stime = Long.valueOf(serverTime);

        //构件输出的value
        TimeOutputWritable timeOutputWritable = new TimeOutputWritable();
        timeOutputWritable.setId(UUID);
        v.setTimeOutputWritable(timeOutputWritable);
        v.setUrl(url);
        v.setUsd(usd);

        //构建key
        DateDimension dimension = DateDimension.buildDate(stime, DateType.DAY);        //自己指定的以 天 为维度的操作
        PlatformDimension platformDimension = new PlatformDimension(platform);
        LocationDimension locationDimension = new LocationDimension();

        locationDimension.setCountry(country);
        locationDimension.setProvince(province);
        locationDimension.setCity(city);

        //添加进入statusCommonDimension操作
        //StatusCommonDimension不能通过new来创建
        StatusCommonDimension statusCommonDimension = this.k.getStatusCommonDimension();

        statusCommonDimension.setDateDimension(dimension);
        statusCommonDimension.setPlatformDimension(platformDimension);

        this.k.setStatusCommonDimension(statusCommonDimension);

        this.k.getStatusCommonDimension().setKpiDimension(pageKpi);
        this.k.setLocationDimension(locationDimension);
        context.write(k, v);


    }
}