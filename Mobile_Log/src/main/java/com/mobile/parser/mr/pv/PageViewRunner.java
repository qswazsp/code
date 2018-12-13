package com.mobile.parser.mr.pv;

import com.mobile.common.GlobalConstants;
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

public class PageViewRunner implements Tool {
    private static final Logger logger = Logger.getLogger(PageViewRunner.class);

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new PageViewRunner(), args);
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
        Job job = Job.getInstance(conf, "new user");

        job.setJarByClass(PageViewRunner.class);
        job.setMapperClass(PageViewMapper.class);
        job.setMapOutputKeyClass(StatusUserDimension.class);
        job.setMapOutputValueClass(TimeOutputWritable.class);

        job.setReducerClass(PageViewReducer.class);
        job.setOutputKeyClass(StatusUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);

        //设置输入输出参数
        handleInputAndOutput(job);

        //设置输出格式类
        job.setOutputFormatClass(SqlFormat.class);
//        job.setOutputFormatClass(SqlFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }


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
