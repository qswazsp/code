package com.mobile.parser.mr;

import com.mobile.common.GlobalConstants;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.mr.nu.NewUserWriter;
import com.mobile.parser.service.DimensionOperateI;
import com.mobile.parser.service.DimensionOperateImpl;
import com.mobile.util.DBUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MyMysqlFormat
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 自定义reduce输出的格式类(将reduce中输出的key和value的数据存储到mysql)
 **/
public class MyMysqlFormat extends OutputFormat<BaseDimension, StatsBaseOutputWritable> {
    private static final Logger logger = Logger.getLogger(MyMysqlFormat.class);
//    DBOutputFormat

    /**
     * 自定义封装输出记录的信息
     */
    public static class MyMysqlRecordWritter extends RecordWriter<BaseDimension, StatsBaseOutputWritable> {
        private Connection conn = null;  //连接
        private Configuration conf = null; //用于查找sql语句
        private DimensionOperateI operateI = null; //获取对应维度的维度id
        //定义两个缓存
        //kpi-ps
        private Map<KpiTypeEnum, PreparedStatement> cache = new HashMap<KpiTypeEnum, PreparedStatement>();  //缓存kpi-ps
        //kpi-kpi对应的ps个数
        private Map<KpiTypeEnum, Integer> batch = new HashMap<KpiTypeEnum, Integer>(); //一个kpi有多个ps时可以执行，或者有多少个kpi就可以批量执行

        public MyMysqlRecordWritter(Connection conn, Configuration conf, DimensionOperateI operateI) {
            this.conn = conn;
            this.conf = conf;
            this.operateI = operateI;
        }

        /**
         * 写方法:
         * 1、先获取kpi，从value中或者key中，统一以value为准
         * 2、用kpi判断缓存中是否存在ps，有则直接取出，没有则从conf中获取
         * 3、为ps赋值
         * 4、将赋值好的ps添加到批处理中
         * 5、批处理执行ps
         * 6、执行完成，关闭资源并批处理执行剩余的ps
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(BaseDimension key, StatsBaseOutputWritable value) throws IOException, InterruptedException {
            PreparedStatement ps = null;

            try {
                //先判断key
                if (key == null) {
                    return;
                }
                //获取kpi
                KpiTypeEnum kpi = value.getKpi();
                int count = 1;
                //判断缓存中是否存在
                if (this.cache.containsKey(kpi)) {
                    ps = this.cache.get(kpi);
                    count = this.batch.get(kpi);
                    count++;
                } else {
                    String sql = this.conf.get(kpi.kipName); //从conf中获取kpi对应的sql
                    ps = this.conn.prepareStatement(sql);
                    //添加到缓存
                    this.cache.put(kpi, ps);
                }
                //对counter做加1
                this.batch.put(kpi, count);
                System.out.println(conf);

                //处理ps的赋值？？
                /*String className = this.conf.get("writer_new_user"); //com.mobile.parser.mr.nu.NewUserWritter
                Class<?> classz = Class.forName(className); //转换成类
                //得赋值的接口对象
                ReduceOutputFormat reduceOutputFormatI = (ReduceOutputFormat) classz.newInstance();*/
                ReduceOutputFormat reduceOutputFormatI = new NewUserWriter();
                //调用该接口的赋值方法进行对应值的赋值
                reduceOutputFormatI.outputWriter(conf, key, value, ps, operateI);

                //批量执行
                if (/*this.batch.size() % 50 == 0 || */this.batch.get(kpi) % 50 == 0) {
                    this.cache.get(kpi).executeBatch(); //批量执行
//                    this.conn.commit(); //提交，自动提交？？？
                    this.cache.remove(kpi); //对于已经执行应该移除
                }
            } catch (Exception e) {
                logger.warn("sql执行异常.", e);
            }
        }

        /**
         * 关闭
         *
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            //关闭
            for (Map.Entry<KpiTypeEnum, PreparedStatement> en : this.cache.entrySet()) {
                try {
                    en.getValue().executeBatch(); //将整个剩余的ps执行一遍
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                   /* //conn对象不为空
                    if(conn != null){
                        conn.commit();
                    }*/
                    //关闭ps
                    for (Map.Entry<KpiTypeEnum, PreparedStatement> e : this.cache.entrySet()) {
                        try {
                            e.getValue().close();
                        } catch (SQLException e1) {
                            //                            //do nonthing
                        } finally {
                            //关闭conn连接对象
                            if (conn != null) {
                                DBUtil.close(conn, null, null);
                            }
                        }
                    }
                }
            }
        }
    }


    @Override
    public RecordWriter<BaseDimension, StatsBaseOutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = DBUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
        DimensionOperateI operateI = new DimensionOperateImpl();
        return new MyMysqlRecordWritter(conn, conf, operateI);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        return new FileOutputCommitter(null,taskAttemptContext);
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext), taskAttemptContext);
    }
}