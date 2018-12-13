package com.mobile.parser.mr.location;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.StatusLocationDimension;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.value.map.MyMaperOutput;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.MyReduceOutput;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.nu.NewUserReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//1.map端的操作是统计uuid的去重个数
//2.统计uuid 中pv为1的个数
//北京    13  website
//
public class LocationReducer extends Reducer<StatusLocationDimension, MyMaperOutput, StatusLocationDimension, MyReduceOutput> {

    private static final Logger logger = Logger.getLogger(NewUserReduce.class);
    private StatusLocationDimension k = new StatusLocationDimension();
    private MyReduceOutput v = new MyReduceOutput();
    private Set<String> uuidSet = new HashSet<>();
    private Set<String> usdSet = new HashSet<>();

    @Override
    protected void reduce(StatusLocationDimension key, Iterable<MyMaperOutput> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> usdMap = new HashMap<>();
        for (MyMaperOutput to : values) {
            //uuid的去重个数 统计活跃用户
            uuidSet.add(to.getTimeOutputWritable().getId());
            //usd的去重个数  统计总会话数
            usdSet.add(to.getUsd());
            if (!usdMap.containsKey(to.getUsd())) {
                usdMap.put(to.getUsd(), 1);
            } else {
                usdMap.put(to.getUsd(),usdMap.get(to.getUsd()) + 1);
            }
        }
        //统计1跳的个数
        int count = 0;
        for (Map.Entry<String, Integer> to : usdMap.entrySet()) {
            if (to.getValue() == 1) {
                count++;
            }
        }
        MapWritable map = new MapWritable();
        //给v赋值
        //map中<usdCount,bounceSession>
        map.put(new IntWritable(usdSet.size()), new IntWritable(count));
        v.setUuidCount(uuidSet.size());
        v.getReduceOutputWritable().setKpi(KpiTypeEnum.LOCATION);
        v.getReduceOutputWritable().setValue(map);

        map.clear();

        uuidSet.clear();
        usdSet.clear();

        context.write(key, v);
    }
}
