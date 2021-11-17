package com.atguigu;

import java.sql.*;
import java.util.Properties;

/**
 * @ClassName MapReduceDemo-ThickClient
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月11日14:41 - 周一
 * @Describe
 */
public class ThickClient {
    public static void main(String[] args) throws SQLException {
        // 1.添加链接
        String url = "jdbc:phoenix:hadoop102:2181";
        // 2.创建配置
        Properties properties = new Properties();
        // 3.添加配置
        // 需要客户端服务端参数保存一致
         properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        // 4.获取连接
        Connection connection = DriverManager.getConnection(url, properties);
        // 5.编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        // 6.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();
        // 7.输出结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }
        // 8.关闭资源
        connection.close();

    }
}
