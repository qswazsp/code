package com.mobile.parser.mr.au;

import com.mobile.common.GlobalConstants;
import com.mobile.etl.tohdfs.ToHdfsRunner;
import com.mobile.parser.modle.dim.StatusUserDimension;
import com.mobile.parser.modle.dim.value.map.TimeOutputWritable;
import com.mobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.mobile.parser.mr.SqlFormat;
import com.mobile.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ActiveUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(ToHdfsRunner.class);

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ActiveUserRunner(), args);
        } catch (Exception e) {
            logger.warn("运行new user runner 异常.", e);
        }
    }

    private Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration conf) {
        //????????????
        this.conf.addResource("/insertquery-mapping.xml");
        this.conf.addResource("insertquery-writter.xml");

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * yarn jar ./*.jar com.mobile.etl.tohdfs.ToHdfsRunner -d 2018-12-04
     * /logs/12/04
     */

    @Override
    public int run(String[] args) throws Exception {
        //获取配置
        Configuration conf = this.getConf();
        //处理参数，会将运行日期存储到conf中
        setArg(args, conf);
        //获取job
        Job job = Job.getInstance(conf, "active user");

        job.setJarByClass(ActiveUserRunner.class);
        job.setMapperClass(ActiveUserMapper.class);
        job.setMapOutputKeyClass(StatusUserDimension.class);
        job.setMapOutputValueClass(TimeOutputWritable.class);

        job.setReducerClass(ActiveUserReduce.class);
        job.setOutputKeyClass(StatusUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);

        //设置输入输出参数
        handleInputAndOutput(job);

        //设置输出格式类
        job.setOutputFormatClass(SqlFormat.class);
//        job.setOutputFormatClass(SqlFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;

        /*if (job.waitForCompletion(true)) {
            computeTotalUser(job);
            return 0;
        } else {
            return 1;
        }*/
    }

    /**
     * 计算新增总用户
     * 1、根据运行日期，获取当前的维度id，同时也获取运行日期前一天的维度id
     * 2、查询当天新增用户，查询前一天的新增总用户，并更新今天的总用户
     * 3、执行更新数据库操作
     *
     * @param job
     */
    /*private void computeTotalUser(Job job) {
        String todayTime = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        long nowTime = TimeUtil.parseString2Long(todayTime);
        long yesterTime = nowTime - GlobalConstants.DAY_OF_MILLISECOND;
        //获取维度
        DateDimension todayDimension = DateDimension.buildDate(nowTime, DateType.DAY);
        DateDimension yesterDimension = DateDimension.buildDate(yesterTime, DateType.DAY);
        //获取维度id
        DimensionOperateI operateI = new DimensionOperateImpl();
        int todayDimensionId = -1;
        int yesterDimensionId = -1;

        todayDimensionId = operateI.getDimensionIdByDimension(todayDimension);
        yesterDimensionId = operateI.getDimensionIdByDimension(yesterDimension);
        Map<String, Integer> info = new ConcurrentHashMap<>();

        //查询昨天新增总用户
        //获取数据库连接
        Connection conn = null;
        PreparedStatement pre = null;
        ResultSet rs = null;

        conn = DBUtil.getConn();
        try {
            if (yesterDimensionId > 0) {
                pre = conn.prepareStatement("select platform_dimension_id , total_install_users from stats_user where date_dimension_id = ?");
                pre.setInt(1, yesterDimensionId);
                rs = pre.executeQuery();
                while (rs.next()) {
                    info.put(rs.getInt("platform_dimension_id") + "", rs.getInt("total_install_users"));
                }
            }
            //查询今天的数据
            if (todayDimensionId > 0) {
                pre = conn.prepareStatement("select platform_dimension_id , new_install_users from stats_user where date_dimension_id = ?");
                pre.setInt(1, todayDimensionId);
                rs = pre.executeQuery();
                while (rs.next()) {
                    //遍历每一个platform加到totalInstallUser中
                    int platformID = rs.getInt("platform_dimension_id");
                    int totalInstallUser = rs.getInt("new_install_users");
                    if (info.containsKey(platformID + "")) {
                        totalInstallUser += info.get(platformID + "");
                    }
                    info.put(platformID + "", totalInstallUser);
                }
            }

            for (Map.Entry<String, Integer> en : info.entrySet()) {
                pre = conn.prepareStatement("insert into stats_user (date_dimension_id,platform_dimension_id,total_install_users," +
                        "created) VALUES (?,?,?,?) on duplicate key update total_install_users=?");
                pre.setInt(1, todayDimensionId);
                pre.setInt(2, Integer.parseInt(en.getKey()));
                pre.setInt(3, en.getValue());
                pre.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                pre.setInt(5, en.getValue());
                pre.execute();
            }
        } catch (SQLException e) {
            logger.warn("新增用户计算异常", e);
        }finally {
            DBUtil.close(conn, pre, rs);

        }


    }*/

    /**
     * 将运行的date设置到conf中
     *
     * @param args
     */
    private void setArg(String[] args, Configuration conf) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                if (i + 1 < args.length) {
                    conf.set(GlobalConstants.RUNNING_DATE, args[i + 1]);
                    break;  //跳出整个循环
                }
            }
        }

        //判断conf中是否存在
        if (conf.get(GlobalConstants.RUNNING_DATE) == null) {
            //给默认值
            conf.set(GlobalConstants.RUNNING_DATE, TimeUtil.getYesterDate());
        }
    }

    /**
     * 设置输入输出参数
     *
     * @param job
     */
    private void handleInputAndOutput(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String fields[] = date.split("-");
        ///logs/12/04
        try {
            Path inputpath = new Path("/ods/" + fields[1] + "/" + fields[2]);
            FileInputFormat.setInputPaths(job, inputpath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常", e);
        }
    }
}
