package com.mobile.util;

import com.mobile.common.DateType;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @ClassName TimeUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 时间工具类
 **/
public class TimeUtil {
    private static final Logger logger = Logger.getLogger(TimeUtil.class);
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    /**
     * 获取昨天的日期，默认格式
     * @return
     */
    public static String getYesterDate(){
        return getYesterDate(DEFAULT_FORMAT);
    }

    /**
     * 获取昨天的日期 2018-12-05
     * @return
     */
    public static String getYesterDate(String pattern){
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(calendar.getTime());
    }

    public static String long2String(long time){
        return long2String(time,DEFAULT_FORMAT);
    }
    public static String long2String(long time,String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String result = sdf.format(new Date(time));
        return result;
    }

    /**
     * 根据传入的String返回long
     * 默认以yyyy-MM-dd解析
     * @param date
     * @return
     */
    public static long string2Long(String date){
        return string2Long(date,DEFAULT_FORMAT);
    }
    public static long string2Long(String date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        long result = 0;
        try {
            result = sdf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }



    public static int getDateInfo(long time, DateType type) {
        Calendar cal = Calendar.getInstance();
        int result = 0;
        switch (type){
            case YEAR:{
                cal.setTime(new Date(time));
                result = cal.get(Calendar.YEAR);
                break;
            }
            case SEASON:{
                cal.setTime(new Date(time));
                result = cal.get(Calendar.MONTH)+1;
               /* if(result==1||result ==2||result==3){
                    result = 1;
                }
                if(result==4||result ==6||result==5){
                    result = 2;
                }
                if(result==7||result ==8||result==9){
                    result = 3;
                }
                if(result==10||result ==11||result==12){
                    result = 4;
                }*/
                result = (result+2)/3;//做一个简单的优化
                break;
            }
            case MONTH:{
                cal.setTime(new Date(time));
                result = cal.get(Calendar.MONTH)+1;
                break;
            }
            case WEEK:{
                cal.setTime(new Date(time));
                result = cal.get(Calendar.WEEK_OF_YEAR);
                break;
            }
            case DAY:{//这个day貌似是从周日为第一天开始计算的
                cal.setTime(new Date(time));
                result = cal.get(Calendar.DAY_OF_MONTH);
                break;
            }
            case HOUR:{
                result = cal.get(Calendar.HOUR_OF_DAY);
                break;
            }
            default:{
                logger.warn("没有匹配项");
            }
        }
        return result;
    }

    public static long getFirstDayOfWeek(long time) {
        Calendar cal  = Calendar.getInstance();
        cal.setTime(new Date(time));
        int day = cal.get(Calendar.DAY_OF_WEEK);
        //System.out.println("这是本周的第几天:"+day);
        cal.add(Calendar.DATE,-day);
        return cal.getTime().getTime();
    }

    public static void main(String[] args) {
        System.out.println(getYesterDate());
        System.out.println(getYesterDate("yyyy/MM/dd"));
    }
}