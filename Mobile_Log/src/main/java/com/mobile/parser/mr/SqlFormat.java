package com.mobile.parser.mr;

import com.mobile.common.GlobalConstants;
import com.mobile.common.KpiTypeEnum;
import com.mobile.parser.modle.dim.base.BaseDimension;
import com.mobile.parser.modle.dim.value.StatsBaseOutputWritable;
import com.mobile.parser.mr.au.ActiveUserWriter;
import com.mobile.parser.mr.location.LocationWriter;
import com.mobile.parser.mr.nm.NewMemberWriter;
import com.mobile.parser.mr.nu.NewUserWriter;
import com.mobile.parser.service.DimensionOperateI;
import com.mobile.parser.service.DimensionOperateImpl;
import com.mobile.util.DBUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlFormat extends OutputFormat<BaseDimension, StatsBaseOutputWritable> {
    private static final Logger logger = Logger.getLogger(SqlFormat.class);

    @Override
    public RecordWriter<BaseDimension, StatsBaseOutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = DBUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
        DimensionOperateI dimensionOperateI = new DimensionOperateImpl();
        return new InnerRecordWriter(conn, conf, dimensionOperateI);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //
    }

    /**
     * hadoop默认提交的方法
     *
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, taskAttemptContext);
    }


    /**
     * 真正执行操作的内部类
     */
    class InnerRecordWriter extends RecordWriter<BaseDimension, StatsBaseOutputWritable> {
        Connection conn = null;         //用于连接数据库
        Configuration conf = null;      //用于获取sql语句
        DimensionOperateI dimensionOperateI = null;         //根据Kpi获取相应的id
        /**
         * 1.定义缓存和计数器        cache 和 batch
         * cache<kpi, pre>     batch<kpi, count>
         */
        Map<KpiTypeEnum, PreparedStatement> cache = new HashMap<>();
        Map<KpiTypeEnum, Integer> batch = new HashMap<>();

        public InnerRecordWriter(Connection conn, Configuration conf, DimensionOperateI dimensionOperateI) {
            this.conn = conn;
            this.conf = conf;
            this.dimensionOperateI = dimensionOperateI;
        }

        //2.write()方法中的内容

        /**
         * 最重要的方法，真正执行写入数据库的操作
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(BaseDimension key, StatsBaseOutputWritable value) throws IOException, InterruptedException {
            PreparedStatement pre = null;
            //=============================(0)在每个方法中都要先考虑判空的操作===============
            //value可以为空     因为有可能当天的指标为0    但key不能为空，没有指标无法统计数据
            if (key == null) {
                return;
            }
            //=================================(0)结束=========================

            //创建kpi
            KpiTypeEnum kpi = value.getKpi();

            //=================================(1)进行缓存判断和处理=====================
            //初始化计数器
            int count = 1;
            //判断缓存中是否有sql语句
            if (cache.containsKey(kpi)) {
                //如果缓存中已存在
                pre = this.cache.get(kpi);
                count = this.batch.get(kpi);
                count++;
            } else {
                try {
                    //没有则从配置文件获取sql语句
                    String sql = conf.get(kpi.kipName);
                    pre = conn.prepareStatement(sql);
                    //添加到缓存
                    cache.put(kpi, pre);
                } catch (SQLException e) {
                    logger.warn("获取将要被添加的pre异常", e);
                }
            }
            batch.put(kpi, count);
            //===================================(1)结束================================
            //=================================(2)进行pre中sql语句的赋值操作=====================
            //从配置文件获取
            try {
//                String className = conf.get(GlobalConstants.PREFIX_WRITTER + kpi.kipName);
//                Class<?> clazz = Class.forName(className);
//                ReduceOutputFormat reduceOutputFormat = (ReduceOutputFormat) clazz.newInstance();
                ReduceOutputFormat reduceOutputFormat = null;
                if (kpi.kipName.equals(KpiTypeEnum.NEW_MEMBER.kipName) || kpi.kipName.equals(KpiTypeEnum.NEW_MEMBER_BROWSER.kipName) || kpi.kipName.equals(KpiTypeEnum.BROWSER_NEW_USER.kipName)) {
                    reduceOutputFormat = new NewMemberWriter();
                } else if (kpi.kipName.equals(KpiTypeEnum.LOCATION.kipName)) {
                    reduceOutputFormat = new LocationWriter();
                } else {
                    reduceOutputFormat = new NewUserWriter();
                }
                //调用该接口的赋值方法进行对应值的赋值
                reduceOutputFormat.outputWriter(conf, key, value, pre, dimensionOperateI);
            } catch (Exception e) {
                logger.warn("找不到配置文件中的类，或者类型强转失败异常", e);
            }
            //=====================================(2)结束=================================
            //===============================(3)进行批处理的判断和操作===========================
            if (this.batch.get(kpi) % 50 == 0) {
                try {
                    this.cache.get(kpi).executeBatch();     //批处理会自动提交
                    this.cache.remove(kpi);
                } catch (SQLException e) {
                    logger.warn("批处理异常", e);
                }
            }


            //=====================================(3)====================================
        }

        //3.关闭操作                            //记得要flush 不够50条内容

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiTypeEnum, PreparedStatement> en : cache.entrySet()) {
                    en.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.warn("关闭连接中flush异常", e);
            } finally {
                for (Map.Entry<KpiTypeEnum, PreparedStatement> e : cache.entrySet()) {
                    try {
                        e.getValue().close();
                    } catch (SQLException e1) {
                        logger.warn("关闭pre出现异常", e1);
                    } finally {
                        if (conn != null) {
                            DBUtil.close(conn, null, null);
                        }
                    }
                }
            }
        }

        /*@Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            //关闭
            for (Map.Entry<KpiTypeEnum,PreparedStatement> en : this.cache.entrySet()){
                try {
                    en.getValue().executeBatch(); //将整个剩余的ps执行一遍
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                   *//* //conn对象不为空
                    if(conn != null){
                        conn.commit();
                    }*//*
                    //关闭ps
                    for (Map.Entry<KpiTypeEnum,PreparedStatement> e : this.cache.entrySet()){
                        try {
                            e.getValue().close();
                        } catch (SQLException e1) {
                            //                            //do nonthing
                        } finally {
                            //关闭conn连接对象
                            if(conn != null){
                                DBUtil.close(conn,null,null);
                            }
                        }
                    }
                }
            }
        }*/
    }


}
