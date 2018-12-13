package com.mobile.parser.mr.nm;

import com.mobile.common.DateType;
import com.mobile.common.EnumEvent;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusCommonDimension;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.base.BroswerDimension;
import com.mobile.parser.modle.dim.base.DateDimension;
import com.mobile.parser.modle.dim.base.KpiDimension;
import com.mobile.parser.modle.dim.base.PlatformDimension;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.mr.au.ActiveUserMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class NewMemberMapper extends Mapper<LongWritable, Text, StatusUserDimension, TimeOutputWritable> {
    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private static StatusUserDimension k = new StatusUserDimension();
    private static TimeOutputWritable v = new TimeOutputWritable();

    private KpiDimension activeKpi = new KpiDimension(KpiTypeEnum.NEW_MEMBER.kipName);
    private KpiDimension activeBrowserKpi = new KpiDimension(KpiTypeEnum.NEW_MEMBER_BROWSER.kipName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        String event = fields[0];
        String nmID = fields[10];
        String serverTime = fields[15];
        String platform = fields[2];
        String browserName = fields[26];
        String browserVersion = fields[27];

        //过滤
        if (!event.equals(EnumEvent.PAGEVIEW.alias)) {
            logger.warn("事件不是pageView事件:" + line);
            return;
        }

        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(nmID)) {
            return;
        }

        long stime = Long.valueOf(serverTime);

        //构件输出的value
        this.v.setId(nmID);

        //构建key
        DateDimension dimension = DateDimension.buildDate(stime, DateType.DAY);        //自己指定的以 天 为维度的操作
        PlatformDimension platformDimension = new PlatformDimension(platform);

        //默认浏览器维度
        BroswerDimension defaultBrowser = new BroswerDimension("", "");

        //添加进入statusCommonDimension操作
        //StatusCommonDimension不能通过new来创建
        StatusCommonDimension statusCommonDimension = this.k.getStatusCommonDimension();

        statusCommonDimension.setDateDimension(dimension);
        statusCommonDimension.setPlatformDimension(platformDimension);
        statusCommonDimension.setKpiDimension(activeKpi);


        this.k.setStatusCommonDimension(statusCommonDimension);
        this.k.setBroswerDimension(defaultBrowser);
        context.write(k, v);


        this.k.getStatusCommonDimension().setKpiDimension(activeBrowserKpi);
        defaultBrowser.setBroswerName(browserName);
        defaultBrowser.setBroswerVersion(browserVersion);
        this.k.setBroswerDimension(defaultBrowser);
        context.write(k, v);
    }
}
