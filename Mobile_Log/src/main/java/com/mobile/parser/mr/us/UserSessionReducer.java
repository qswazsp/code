package com.mobile.parser.mr.us;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.nu.NewUserReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserSessionReducer extends Reducer<StatusUserDimension, TimeOutputWritable, StatusUserDimension, ReduceOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewUserReduce.class);
    private StatusUserDimension k = new StatusUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();
    private Set<String> set = new HashSet();

    //web 9  ie 8   123  1111
    //web 9  ie 8   123  2222
    //web 9  ie 8   152  3333
    //web 9  ie 8   152  3344
    @Override
    protected void reduce(StatusUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {

        MapWritable map = new MapWritable();
        Map<String, Long> map2 = new HashMap<>();
        Long sessionLength = 0l;
        String mapKey = "";
        Long mapValue = 0l;
        for (TimeOutputWritable to : values) {
            set.add(to.getId());
            //存放第一次的sid的时间值
            mapKey = to.getId();
            mapValue = to.getTime();
            if (!map2.containsKey(mapKey)) {
                map2.put(mapKey, mapValue);
            } else {
                Long mapValue2 = map2.get(mapKey);
                Long sessionLength2 = mapValue - mapValue2;
                sessionLength += Math.abs(sessionLength2);
            }
        }
        map.put(new IntWritable(this.set.size()),new LongWritable(sessionLength));
        v.setValue(map);
        String kpiName = key.getStatusCommonDimension().getKpiDimension().getKpiName();
        if (kpiName.equals(KpiTypeEnum.SESSION_BROWSER.kipName)) {
            v.setKpi(KpiTypeEnum.SESSION_BROWSER);
        } else if (kpiName.equals(KpiTypeEnum.SESSION.kipName)) {
            v.setKpi(KpiTypeEnum.SESSION);
        }
        set.clear();

        context.write(key, v);
    }
}
