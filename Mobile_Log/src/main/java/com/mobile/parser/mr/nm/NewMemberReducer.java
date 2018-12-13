package com.mobile.parser.mr.nm;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.nu.NewUserReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;

public class NewMemberReducer extends Reducer<StatusUserDimension, TimeOutputWritable, StatusUserDimension, ReduceOutputWritable> {

    private static final Logger logger = Logger.getLogger(NewUserReduce.class);
    private StatusUserDimension k = new StatusUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();

    @Override
    protected void reduce(StatusUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable map = new MapWritable();
        for (TimeOutputWritable to : values) {
            map.put(new IntWritable((int)Double.parseDouble(to.getId())), new IntWritable(-1));
        }
        v.setValue(map);
        String kpiName = key.getStatusCommonDimension().getKpiDimension().getKpiName();
        if (kpiName.equals(KpiTypeEnum.NEW_MEMBER.kipName)) {
            v.setKpi(KpiTypeEnum.NEW_MEMBER);
        } else if (kpiName.equals(KpiTypeEnum.NEW_MEMBER_BROWSER.kipName)) {
            v.setKpi(KpiTypeEnum.NEW_MEMBER_BROWSER);
        }

        context.write(key, v);
    }
}
