package com.mobile.parser.mr.nu;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewUserReduce extends Reducer<StatusUserDimension, TimeOutputWritable, StatusUserDimension, ReduceOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewUserReduce.class);
    private StatusUserDimension k = new StatusUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();
    private Set<String> set = new HashSet();

    @Override
    protected void reduce(StatusUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {
        for (TimeOutputWritable to : values) {
            set.add(to.getId());
        }
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(this.set.size()));
        v.setValue(map);
        String kpiName = key.getStatusCommonDimension().getKpiDimension().getKpiName();
        if (kpiName.equals(KpiTypeEnum.TOTAL_NEW_USER.kipName)) {
            v.setKpi(KpiTypeEnum.TOTAL_NEW_USER);
        } else if (kpiName.equals(KpiTypeEnum.NEW_USER.kipName)) {
            v.setKpi(KpiTypeEnum.NEW_USER);
        }
        set.clear();

        context.write(key, v);
    }
}
