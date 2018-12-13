package com.mobile.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UserAgentUtil {
    //创建一个Log日志对象
    private final static Logger logger = Logger.getLogger(UserAgentUtil.class);
    //创建解析agent的实例类
    private static UASparser uaSparser = null;
    //初始化
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("初始化userAgent解析对象异常" , e);
        }
    }

    public static UserAgentInfo parserInfoAgent (String userAgent) {
        UserAgentInfo info = null;
        // 如果 userAgent 为空
        if (StringUtils.isEmpty(userAgent)) {
            return info;
        }
        //浏览器代理不为空
        try {
            cz.mallat.uasparser.UserAgentInfo uinfo = uaSparser.parse(userAgent);
            //取出uinfo中的属性值设置到info中
            info = new UserAgentInfo();
            info.setBrowserName(uinfo.getUaFamily());
            info.setBrowserVersion(uinfo.getBrowserVersionInfo());
            info.setOsName(uinfo.getOsName());
            info.setOsVersion(uinfo.getOsFamily());
        } catch (IOException e) {
            logger.warn("userparser解析useragent异常",e);
        }
        return info;
    }


    /**
     * 用于封装useragent被解析后的信息
     *
     */
    public static class UserAgentInfo {
    private String browserName;
    private String browserVersion;
    private String osName;
    private String osVersion;

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    browserName + '\t' +
                    browserVersion + '\t' +
                    osName + '\t' +
                    osVersion + '\t' +
                    '}';
        }
    }

}
