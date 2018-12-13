package com.mobile.etl.util;

import com.mobile.etl.util.ip.IPSeeker;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class IpUtil {
    private static final Logger logger = Logger.getLogger(IpUtil.class);
    static RegionInfo info = null;

    //初始化
    public static RegionInfo parserIp(String ip) {
        //判断是否为空
        if (StringUtils.isEmpty(ip)) {
            return info;
        }
        //ip不为空
        String country = IPSeeker.getInstance().getCountry(ip);
        if (StringUtils.isNotEmpty(country)) {
            info = new RegionInfo();
            if (country.equals("局域网")) {
                info.setCountry("中国");
                info.setProvince("北京市");
                info.setCity("昌平区");
            }

            //自己的代码
             /*
            else {
               if (!country.contains("省")) {
                    if (country.contains("北京")) {
                        info.setCountry("中国");
                        info.setProvince("北京市");
                        info.setCity("北京市");
                    } else if (country.contains("天津")) {
                        info.setCountry("中国");
                        info.setProvince("天津市");
                        info.setCity("天津市");
                    } else if (country.contains("广西") || country.contains("宁夏") || country.contains("新疆") || country.contains("西藏")) {
                        info.setCountry("中国");
                        info.setProvince(country.substring(0, 2) + "自治区");
                        info.setCity(country.substring(2, country.length()));
                    } else if (country.contains("内蒙古")) {
                        info.setCountry("中国");
                        info.setCity(country.substring(3, country.length()));
                        info.setProvince(country.substring(0, 3) + "自治区");
                    }
                } else {

                    String[] split = country.split("省");
                    info.setCountry("中国");
                    info.setProvince(split[0] + "省");
                    info.setCity(split[1]);
                }
                */

            //老师的代码
            else if (country != null && !country.trim().equals("")) {
                //定位"省"的位置
                int index = country.indexOf("省");
                //大于0代表省份存在，小于0代表是直辖市、特区或自治区等
                if (index > 0) {
                    info.setCountry("中国");
                    info.setProvince(country.substring(0, index + 1));
                    //定位市
                    int index1 = country.indexOf("市");
                    // 查看市是否存在
                    if (index1 > 0) {
                        info.setCity(country.substring(index + 1, index1 + 1));
                    }
                } else {
                    //代表省不存在    country 内为直辖市、特区或者自治区
                    String flag = country.substring(0, 2);
                    switch (flag) {
                        //如果为自治区
                        case "内蒙":
                            info.setCountry("中国");
                            info.setProvince(flag + "古");
                            //设置市
                            country = country.substring(0, 3);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(3, index + 1));
                            }
                            break;
                        case "广西":
                        case "新疆":
                        case "宁夏":
                        case "西藏":
                            info.setCountry("中国");
                            info.setProvince(flag);
                            //设置市
                            country = country.substring(0, 2);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(2, index + 1));
                            }
                            break;
                        //当为直辖市时
                        case "北京":
                        case "天津":
                        case "上海":
                        case "重庆":
                            info.setCountry("中国");
                            info.setProvince(flag + "市");
                            country = country.substring(0, 2);
                            //设置区
                            index = country.indexOf("区");
                            if (index > 0) {
                                char c = country.charAt(index - 1);
                                if (c != '小' && c != '校' && c != '军') {
                                    info.setCity(country.substring(0, index + 1));
                                }
                            }
                            else {
                                //如果没有区，有县
                                index = country.indexOf("县");
                                if (index > 0) {
                                    info.setCity(country.substring(0, index + 1));
                                }
                            }
                            break;

                        //如果为特区
                        case "香港":
                        case "澳门":
                            info.setCountry("中国");
                            info.setProvince(flag + "特别行政区");
                            break;
                        case "台湾":
                            info.setCountry("中国");
                            info.setProvince(flag + "省");
                            break;
                        default:
                            info.setCountry(country);
                            break;
                    }
                }
            }
        }
        return info;
    }


    public static class RegionInfo {
        private static String DEFAULT = "unknown";
        private String country = DEFAULT;
        private String province = DEFAULT;
        private String city = DEFAULT;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    country + '\t' +
                    province + '\t' +
                    city + '\t' +
                    '}';
        }
    }
}

