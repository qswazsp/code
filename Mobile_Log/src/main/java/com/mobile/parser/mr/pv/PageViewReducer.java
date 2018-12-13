package com.mobile.parser.mr.pv;

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

public class PageViewReducer extends Reducer<StatusUserDimension, TimeOutputWritable, StatusUserDimension, ReduceOutputWritable> {

    private static final Logger logger = Logger.getLogger(NewUserReduce.class);
    private StatusUserDimension k = new StatusUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();

    @Override
    protected void reduce(StatusUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable map = new MapWritable();
        int count = 0;
        for (TimeOutputWritable to : values) {
            count++;
        }
        map.put(new IntWritable(-1), new IntWritable(count));
        v.setValue(map);
        String kpiName = key.getStatusCommonDimension().getKpiDimension().getKpiName();
        if (kpiName.equals(KpiTypeEnum.PAGEVIEW.kipName)) {
            v.setKpi(KpiTypeEnum.PAGEVIEW);
        }
        context.write(key, v);
    }
}