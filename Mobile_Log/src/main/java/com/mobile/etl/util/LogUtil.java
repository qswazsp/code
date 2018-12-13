package com.mobile.etl.util;

import com.mobile.common.LogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 将hdfs中的数据的每一行都进行ip、useragent、时间戳等的解析
 **/
public class LogUtil {
    private static Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 192.168.216.1^A1499154713.194^A192.168.216.123^A
     * /1603.JPG?c_time=1499154713461&oid=123458&u_mid=678678&pl=java_server&en=e_cr&sdk=jdk&ver=1
     *
     * @param log 一行日志
     * @return 将解析后的k-v存储到map中，以便etl的mapper进行使用
     */
    public static Map parserLog(String log) {
        //当log为空时返回null
        if (StringUtils.isEmpty(log)) {
            return null;
        }
        //log不为空
        String[] line = log.split(LogConstants.LOG_SEPARATOR);
        //map里面存放的是每节的日志
        Map<String, String> map = new ConcurrentHashMap<>();
        //line的长度为4
        if (line.length == 4) {
            map.put(LogConstants.LOG_IP, line[0]);
            map.put(LogConstants.LOG_SERVER_TIME, line[1].replaceAll("\\.", ""));
            //解析requestBody
            handleRequestBody(line[3], map);
            //解析Ip
            handleIp(map);
            //解析userAgent
            handleUserAgent(map);
        }
        return map;
    }

    private static void handleUserAgent(Map<String, String> map) {
        if (!map.isEmpty()) {
            //获取userAgent
            String userAgent = map.get(LogConstants.LOG_USER_AGENT);
            //解析字段
            UserAgentUtil.UserAgentInfo info = UserAgentUtil.parserInfoAgent(userAgent);
            //将数据加载到map中
            map.put(LogConstants.LOG_BROWSERNAME, info.getBrowserName());
            map.put(LogConstants.LOG_BROWSERVERSION, info.getBrowserVersion());
            map.put(LogConstants.LOG_OSNAME, info.getOsName());
            map.put(LogConstants.LOG_OSVERSION, info.getBrowserVersion());

        }



    }

    private static void handleIp(Map<String, String> map) {
        if (!map.isEmpty()) {
            //获取ip
            String ip = map.get(LogConstants.LOG_IP);
            //调用ip的解析方法
            IpUtil.RegionInfo rinfo = IpUtil.parserIp(ip);
            //将rinfo 中的属性存储
            map.put(LogConstants.LOG_COUNTRY, rinfo.getCountry());
            map.put(LogConstants.LOG_PROVINCE, rinfo.getProvince());
            map.put(LogConstants.LOG_CITY, rinfo.getCity());


        }




    }


    private static void handleRequestBody(String request, Map<String, String> map) {
        try {
            if (StringUtils.isNotEmpty(request)) {
                //获取 ? 的定位
                int index = request.indexOf("?");
                if (index > 0) {
                    String body = request.substring(index + 1, request.length());
                    String[] kvs = body.split("&");
                    for (String kv : kvs) {
                        String kv1[] = kv.split("=");
                        String k = kv1[0];
                        String v = URLDecoder.decode(kv1[1], "UTF-8");
                        //判断key是否为空过滤
                        if (k != null && !k.trim().equals("")) {
                            //将key-value存储到map中
                            map.put(k, v);
                        }
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.warn("value解码异常.", e);
        }

    }
}