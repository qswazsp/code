package com.mobile.etl.tohdfs;

import com.mobile.common.GlobalConstants;
import com.mobile.util.FSUtil;
import com.mobile.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


public class ToHdfsRunner implements Tool {

    private static Logger logger = Logger.getLogger(ToHdfsRunner.class);
    private Configuration configuration = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ToHdfsRunner(), args);
        } catch (Exception e) {
            logger.warn("运行ToHdfsRunner异常");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //初始化conf参数
        Configuration conf = this.getConf();
        //设置输入输出
        setArgs(args, conf);

        //获取job实例对象
        Job job = Job.getInstance(conf, "toHdfs");
        //获取运行的类
        job.setJarByClass(ToHdfsRunner.class);
        //获取mapper类
        job.setMapperClass(ToHdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输入输出参数
        handleInputAndOutput(job);

        //设置reducer
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 设置输入输出参数
     *
     * @param job
     */
    private void handleInputAndOutput(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String[] fields = date.split("-");

        try {
            Path inputPath = new Path("/logs/" + fields[1] + "/" + fields[2]);
            Path outputPath = new Path("/ods/" + fields[1] + "/" + fields[2]);


            FileInputFormat.addInputPath(job, inputPath);
            FileSystem fs = FSUtil.getFS();
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常", e);
        }



    }

    /**
     * 将运行中的date添加到conf中
     *
     * @param args
     * @param conf
     */
    private void setArgs(String[] args, Configuration conf) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                if (i + 1 < args.length) {
                    conf.set(GlobalConstants.RUNNING_DATE, args[i + 1]);
                    break;
                }
            }
        }
        //判断conf中是否存在-d
        if (conf.get(GlobalConstants.RUNNING_DATE) == null) {
            //给默认值
            conf.set(GlobalConstants.RUNNING_DATE, TimeUtil.getYesterDate());

        }

    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = new Configuration();

    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }




}
