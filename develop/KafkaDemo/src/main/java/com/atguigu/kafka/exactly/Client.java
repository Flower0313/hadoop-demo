package com.atguigu.kafka.exactly;

import java.sql.SQLException;

/**
 * @ClassName MapReduceDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月19日16:38 - 周三
 * @Describe
 */
public class Client {
    public static void main(String[] args) throws SQLException {
        AccurateConsumer acc = new AccurateConsumer("test");
        acc.receiveMsg();
    }
}
