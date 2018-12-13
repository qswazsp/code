package com.mobile.util;
import com.mobile.common.GlobalConstants;

import java.sql.*;

public  class DBUtil {

    //静态加载驱动
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL, GlobalConstants.USER, GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void close(Connection conn, PreparedStatement pre, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
        if (pre != null) {
            try {
                pre.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
    }
}
