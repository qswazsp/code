package com.mobile.parser.mr.nu;

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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 新增用户的的mapper类
 * <p>
 * 新增用户：统计lanuch事件中uuid的去重的个数。涉及维度：date/platform/broswer
 * <p>
 * 需要的字段:
 * uuid/s_time/pl/
 */
public class NewUserMapper extends Mapper<LongWritable, Text, StatusUserDimension, TimeOutputWritable> {
    private static Logger logger = Logger.getLogger(NewUserMapper.class);
    private static StatusUserDimension k = new StatusUserDimension();
    private static TimeOutputWritable v = new TimeOutputWritable();

    private KpiDimension newInstallUserKpi = new KpiDimension(KpiTypeEnum.NEW_USER.kipName);
    private KpiDimension totalNewInstallUserKpi = new KpiDimension(KpiTypeEnum.TOTAL_NEW_USER.kipName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        String event = fields[0];
        String usd = fields[9];
        String cTime = fields[10];
        String serverTime = fields[15];
        String platform = fields[2];
        List<PlatformDimension> platformList = PlatformDimension.buildList(platform);


        //过滤
        if (!event.equals(EnumEvent.LANUCH.alias)) {
            logger.warn("事件不是launch事件:" + line);
            return;
        }
        //uuid 和时间戳
        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(usd)) {
            logger.warn("stime is null and uid is null.sTime:" + serverTime + " uid:" + usd);
            return;
        }
        long longOfTime = Long.valueOf(serverTime);

        //构件输出的value
        this.v.setId(usd);

        //构建key
        DateDimension dimension = DateDimension.buildDate(longOfTime, DateType.DAY);        //自己指定的以 天 为维度的操作
        PlatformDimension platformDimension = new PlatformDimension("total");
        platformList.add(platformDimension);
        //StatusCommonDimension不能通过new来创建
        StatusCommonDimension statusCommonDimension = this.k.getStatusCommonDimension();

        //默认浏览器维度
        BroswerDimension defaultBrowser = new BroswerDimension("", "");

        for (PlatformDimension platformDimension1 : platformList) {

            statusCommonDimension.setDateDimension(dimension);
            statusCommonDimension.setPlatformDimension(platformDimension1);
            if (platformDimension1.getPlatformName().equals("total")) {
                statusCommonDimension.setKpiDimension(totalNewInstallUserKpi);
            } else {
                statusCommonDimension.setKpiDimension(newInstallUserKpi);
            }

            this.k.setStatusCommonDimension(statusCommonDimension);
            this.k.setBroswerDimension(defaultBrowser);

            context.write(k, v);
        }

    }
}
