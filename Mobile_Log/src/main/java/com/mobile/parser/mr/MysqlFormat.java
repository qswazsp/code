package com.mobile.parser.mr;

import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
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
 * 自定义reduce输出的格式类(将reduce中输出的Key和value的数据存储到mysql中)
 */
public class MysqlFormat extends OutputFormat<BaseDimension, StatsBaseOutputWritable> {
    private static final Logger logger = Logger.getLogger(MysqlFormat.class);
//    DBOutputFormat





    /**
     * 自定义内部类
     * 自定义封装输出记录的信息
     */
    public static class MysqlRecordWriter extends RecordWriter<BaseDimension, StatsBaseOutputWritable> {
        private Connection conn = null;     //数据库连接
        private Configuration conf = null;      //用于查找sql语句
        private DimensionOperateI operateI = null;          //获取对应维度的对应id
        //定义缓存

        private Map<KpiTypeEnum, PreparedStatement> cache = new HashMap<>();      //缓存kpi-pre
        //定义计数器
        private Map<KpiTypeEnum, Integer> batch = new HashMap<>();      //当一个Kpi有多个pre时可以执行或者有多少个kpi可以批量执行

        public MysqlRecordWriter(Connection conn, Configuration conf, DimensionOperateI operateI) {
            this.conn = conn;
            this.conf = conf;
            this.operateI = operateI;
        }

        /**
         * 写方法  (核心方法)
         * 1.先获取kpi，从value中key中，同一以value为准
         * 2.用kpi判断缓存中是否存在pre，有则直接取出，没有则从conf中获取
         * 3.为pre赋值
         * 4.将赋值好的pre添加到批处理中
         * 5.批处理执行ps
         * 6.执行完成，关闭资源并批处理执行剩余的ps
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override

        public void write(BaseDimension key, StatsBaseOutputWritable value) throws IOException, InterruptedException {
            try {
                PreparedStatement pre = null;

                //先判断key,value是否为空
                if (key == null && value == null) {
                    logger.warn("");
                    return;
                }

                //==================================进行缓存处理=====================
                //获取kpi
                KpiTypeEnum kpi = value.getKpi();
                int count = 1;
                //判断缓存中是否存在
                if (this.cache.containsKey(kpi)) {
                    pre = this.cache.get(kpi);
                    count = this.batch.get(kpi);
                    count++;
                } else {
                    pre = this.conn.prepareStatement(this.conf.get(kpi.kipName));       //从conf中获取kpi对应的sql
                    //添加到缓存
                    this.cache.put(kpi, pre);
                }
                this.batch.put(kpi, count);

                //===============================缓存处理结束========================
                //=============================进行pre中sql语句的赋值操作==================
                //处理pre 的赋值
                String className = this.conf.get(kpi.kipName);      //com.
                Class<?> clazz = Class.forName(className);      //转换成类
                //得到赋值的接口对象
                ReduceOutputFormat reduceOutputFormat = (ReduceOutputFormat) clazz.newInstance();
                //调用该接口的赋值方法进行对应值的赋值
                reduceOutputFormat.outputWriter(conf, key, value, pre, operateI);

                //============================sql语句赋值操作结束===========================

                //============================进行批量操作的判断,并执行操作=====================
                //批量执行
                if (/*this.batch.size() % 50 == 0 || */this.batch.get(kpi) % 50 == 0) {
                    this.cache.get(kpi).executeBatch();        //批量执行
//                    this.conn.commit();     //提交，自动提交
                    this.cache.remove(kpi);     //对于已经执行应该移除
                }
            } catch (Exception e) {
                logger.warn("sql执行异常", e);
            }
            //=================================判断结束===============================


        }

        /**
         * 关闭资源
         *
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            //关闭资源
            //关闭pre         有问题，顺序不对
            for (Map.Entry<KpiTypeEnum, PreparedStatement> e : this.cache.entrySet()) {
                try {
                    e.getValue().close();
                } catch (SQLException e1) {
                    //do nothing
                } finally {
                    for (Map.Entry<KpiTypeEnum, PreparedStatement> en : this.cache.entrySet()) {
                        try {
                            en.getValue().executeBatch();           //将整个剩余的pre执行一遍
                        } catch (SQLException ex) {
                            ex.printStackTrace();
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

        return new MysqlRecordWriter(conn, conf, operateI);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出空间

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        return new FileOutputCommitter(null, taskAttemptContext);
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext), taskAttemptContext);

    }

}
