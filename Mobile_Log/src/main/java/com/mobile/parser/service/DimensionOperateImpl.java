package com.mobile.parser.service;

import com.mobile.parser.modle.dim.base.*;
import com.mobile.util.DBUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作基础维度类的接口实现
 */
public class DimensionOperateImpl implements DimensionOperateI {

    private static Logger logger = Logger.getLogger(DimensionOperateImpl.class);

    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     * 根据dimension获取对应的维度id
     * 如果dimension在mysql中存在，则查询出id值，如果不存在，则插入后获取其id值
     * 在map或reduce阶段可以直接传入最高基类，然后返回具体的数值
     *
     * @param dimension
     * @return
     */
    @Override
    public int getDimensionIdByDimension(BaseDimension dimension) {
        String cacheKey = buildCacheKey(dimension);
        //判断缓存中是否有对应的cachekey
        if (cacheKey != null) {
            if (this.cache.containsKey(cacheKey)) {
                return this.cache.get(cacheKey);
            }
        }
        //缓存中没有对应的id,则操作mysql
        Connection conn = DBUtil.getConn();

        //制作mysql语句
        String[] sqls = null;
        if (dimension instanceof BroswerDimension) {
            sqls = buildBroswerSqls();
        } else if (dimension instanceof PlatformDimension) {
            sqls = buildPlatformSqls();
        } else if (dimension instanceof KpiDimension) {
            sqls = buildKpiSqls();
        } else if (dimension instanceof DateDimension) {
            sqls = buildDateSqls();
        } else if (dimension instanceof LocationDimension) {
            sqls = buildLocationSqls();
        } else {
            throw new RuntimeException("该dimension不存在,所以不能构建cacheKey");
        }


        //执行sql
        int id = -1;
        synchronized (DimensionOperateI.class) {
            id = execcutedSql(sqls, conn, dimension);
        }

        //将cache和id存储到缓存中
        this.cache.put(cacheKey, id);
        return id;
    }

    private String[] buildLocationSqls() {
        String selectsql = "select id from `dimension_location` where `country` = ? and `province` = ? and `city` = ?";
        String insertSql = "insert into `dimension_location`(`country`,`province`,`city`) values(?,?,?)";
        return new String[]{selectsql, insertSql};
    }

    private String[] buildPlatformSqls() {
        String selectsql = "select id from `dimension_platform` where `platform_name` = ?";
        String insertSql = "insert into `dimension_platform`(`platform_name`) values(?)";
        return new String[]{selectsql, insertSql};
    }


    private String[] buildBroswerSqls() {
        String selectsql = "select id from `dimension_browser` where `browser_name` = ? and  `browser_version` = ?";
        String insertSql = "insert into `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{selectsql, insertSql};
    }

    private String[] buildKpiSqls() {
        String selectsql = "select id from `dimension_kpi` where `kpi_name` = ?";
        String insertSql = "insert into `dimension_kpi`(`kpi_name`) values(?)";
        return new String[]{selectsql, insertSql};
    }

    private String[] buildDateSqls() {
        String selectsql = "select id from `dimension_date` where `year` = ? and  `season` = ? and `month` = ? and `week` = ? and `day` = ? and `calendar` = ? and `type` = ?";
        String insertSql = "insert into `dimension_date`(`year`,`season`,`month`,`week`,`day`,`calendar`,`type`) values(?,?,?,?,?,?,?)";
        return new String[]{selectsql, insertSql};
    }

    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if (dimension instanceof BroswerDimension) {
            sb.append("browser_");
            BroswerDimension broswerDimension = (BroswerDimension) dimension;
            sb.append(broswerDimension.getBroswerName()).append(broswerDimension.getBroswerVersion());
        } else if (dimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension) dimension;
            sb.append(kpiDimension.getKpiName());
        } else if (dimension instanceof PlatformDimension) {
            sb.append("pl_");
            PlatformDimension platformDimension = (PlatformDimension) dimension;
            sb.append(platformDimension.getPlatformName());
        } else if (dimension instanceof LocationDimension) {
            sb.append("lo_");
            LocationDimension locationDimension = (LocationDimension) dimension;
            sb.append(locationDimension.getCountry())
                    .append(locationDimension.getProvince())
                    .append(locationDimension.getCity());
        } else if (dimension instanceof DateDimension) {
            sb.append("date_");
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append(dateDimension.getYear())
                    .append(dateDimension.getSeason())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getWeek())
                    .append(dateDimension.getDay())
                    .append(dateDimension.getType());
        } else {
            throw new RuntimeException("该dimension不存在.所以不能构建cachekey.");
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private int execcutedSql(String[] sqls, Connection conn, BaseDimension dimension) {
        PreparedStatement pre = null;
        ResultSet rs = null;


        //先执行查询
        try {
            pre = conn.prepareStatement(sqls[0]);
            //为ps赋值
            handleArgs(pre, dimension);
            rs = pre.executeQuery();
            if (rs.next()) {
                return rs.getInt("id");
            }

            pre = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            handleArgs(pre, dimension);
            pre.executeUpdate();
            rs = pre.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }

        } catch (SQLException e) {
            logger.warn("执行获取维度id异常.", e);
        }


        return -1;
    }

    /**
     * 为preparedStatement赋值
     *
     * @param pre
     * @param dimension
     */
    private void handleArgs(PreparedStatement pre, BaseDimension dimension) {

        try {
            int i = 0;
            if (dimension instanceof BroswerDimension) {
                BroswerDimension br = (BroswerDimension) dimension;
                pre.setString(++i, br.getBroswerName());
                pre.setString(++i, br.getBroswerVersion());
            } else if (dimension instanceof PlatformDimension) {
                PlatformDimension pd = (PlatformDimension) dimension;
                pre.setString(++i, pd.getPlatformName());
            } else if (dimension instanceof LocationDimension) {
                LocationDimension ld = (LocationDimension) dimension;
                pre.setString(++i, ld.getCountry());
                pre.setString(++i, ld.getProvince());
                pre.setString(++i, ld.getCity());
            } else if (dimension instanceof DateDimension) {
                DateDimension dd = (DateDimension) dimension;
                pre.setInt(++i, dd.getYear());
                pre.setInt(++i, dd.getSeason());
                pre.setInt(++i, dd.getMonth());
                pre.setInt(++i, dd.getWeek());
                pre.setInt(++i, dd.getDay());
                pre.setDate(++i, new Date(dd.getCalendar().getTime()));
                pre.setString(++i, dd.getType());
            } else if (dimension instanceof KpiDimension) {
                KpiDimension kd = (KpiDimension) dimension;
                pre.setString(++i, kd.getKpiName());
            } else {
                throw new RuntimeException("不能赋值参数");
            }
        } catch (SQLException e) {
            logger.warn("为参数赋值异常");
        }


    }

}
