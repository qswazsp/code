package com.mobile.etl.tohdfs;

import com.mobile.common.EnumEvent;
import com.mobile.common.LogConstants;
import com.mobile.etl.util.LogUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.hsqldb.lib.StringUtil;

import java.io.IOException;
import java.util.Map;

public class ToHdfsMapper extends Mapper<Object, Text, LogWritable, NullWritable> {
    private static final Logger logger = Logger.getLogger(ToHdfsMapper.class);
    private LogWritable k = new LogWritable();
    private int inputRecords, outputRecords, fileterRecords = 0;


    public void map(Object key, Text value, Context context) throws IOException {
        String line = value.toString();
        if (StringUtil.isEmpty(line)) {
            this.fileterRecords++;
            return;
        }
        try {
            //正常情况
            Map<String, String> info = LogUtil.parserLog(line);


            //可以根据具体的事件来具体处理
            String eventName = info.get(LogConstants.LOG_EVENT);
            EnumEvent event = EnumEvent.valueOfAlias(eventName);

            switch (event) {
                case LANUCH:
                case PAGEVIEW:
                case CHARGE_REFUND:
                case CHARGE_REQUEST:
                case CHARGE_SUCCESS:
                case EVENT:

                    this.k.setEn(info.getOrDefault(LogConstants.LOG_EVENT,""));
                    this.k.setVer(info.getOrDefault(LogConstants.LOG_VERSION,""));
                    this.k.setPl(info.getOrDefault(LogConstants.LOG_PLATFORM,""));
                    this.k.setSdk(info.getOrDefault(LogConstants.LOG_SDK,""));
                    this.k.setB_rst(info.getOrDefault(LogConstants.LOG_RESOULATION,""));
                    this.k.setB_iev(info.getOrDefault(LogConstants.LOG_USER_AGENT,""));
                    this.k.setU_ud(info.getOrDefault(LogConstants.LOG_UUID,""));
                    this.k.setL(info.getOrDefault(LogConstants.LOG_LANGUAGE,""));
                    this.k.setU_mid(info.getOrDefault(LogConstants.LOG_MEMBERID,""));
                    this.k.setU_sd(info.getOrDefault(LogConstants.LOG_SESSIONID,""));
                    this.k.setC_time(info.getOrDefault(LogConstants.LOG_CLIENT_TIME,""));
                    this.k.setP_url(info.getOrDefault(LogConstants.LOG_CURRENT_URL,""));
                    this.k.setP_ref(info.getOrDefault(LogConstants.LOG_PREFIX_URL,""));
                    this.k.setTt(info.getOrDefault(LogConstants.LOG_TITLE,""));
                    this.k.setIp(info.getOrDefault(LogConstants.LOG_IP,""));
                    this.k.setS_time(info.getOrDefault(LogConstants.LOG_SERVER_TIME,""));
                    this.k.setCa(info.getOrDefault(LogConstants.LOG_CATEGORY,""));
                    this.k.setAc(info.getOrDefault(LogConstants.LOG_ACTION,""));
                    this.k.setKv(info.getOrDefault(LogConstants.LOG_KEY_VALUE,""));
                    this.k.setDu(info.getOrDefault(LogConstants.LOG_DURATION,""));
                    this.k.setOid(info.getOrDefault(LogConstants.LOG_ORDERID,""));
                    this.k.setOn(info.getOrDefault(LogConstants.LOG_ORDER_NAME,""));
                    this.k.setCua(info.getOrDefault(LogConstants.LOG_CURRENCY_AMOUNT,""));
                    this.k.setCut(info.getOrDefault(LogConstants.LOG_CLIENT_TIME,""));
                    this.k.setPt(info.getOrDefault(LogConstants.LOG_PAYMENT,""));
                    this.k.setBrowser_name(info.getOrDefault(LogConstants.LOG_BROWSERNAME,""));
                    this.k.setBrowser_version(info.getOrDefault(LogConstants.LOG_BROWSERVERSION,""));
                    this.k.setOs_name(info.getOrDefault(LogConstants.LOG_OSNAME,""));
                    this.k.setOs_version(info.getOrDefault(LogConstants.LOG_OSVERSION,""));
                    this.k.setCountry(info.getOrDefault(LogConstants.LOG_COUNTRY,""));
                    this.k.setProvince(info.getOrDefault(LogConstants.LOG_PROVINCE,""));
                    this.k.setCity(info.getOrDefault(LogConstants.LOG_CITY,""));

                    break;

            }
        } catch (Exception e) {
            this.fileterRecords ++;
        }
        //输出

        this.outputRecords ++;
        try {
            context.write(this.k , NullWritable.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    protected void cleanup(Context context) throws IOException,InterruptedException {
        logger.info("==========inputRecords:" + inputRecords + " outputRecords"
                +outputRecords + "fileterRecords "+fileterRecords +"=================");


    }



}
