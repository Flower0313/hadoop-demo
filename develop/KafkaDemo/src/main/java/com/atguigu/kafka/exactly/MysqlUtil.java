package com.atguigu.kafka.exactly;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @ClassName MapReduceDemo-MysqlUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月19日15:34 - 周三
 * @Describe
 */
public class MysqlUtil {
    //mysql连接参数
    public static final String MYSQL_SERVER = "jdbc:mysql://hadoop102:3306/test?characterEncoding=utf-8&useSSL=false";

    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    private static Connection conn;

    public static Connection init() {
        try {
            //mysql连接
            Class.forName(MYSQL_DRIVER);
            conn = DriverManager.getConnection(MYSQL_SERVER, "root", "root");
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Mysql连接失败！");
        }
    }

    public static void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("关闭mysql失败");
        }
    }


}
