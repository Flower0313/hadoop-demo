package com.atguigu.hbase.bigdata;

import com.atguigu.hbase.utils.HBaseUtils_old;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TestHbaseAPI_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月04日23:23 - 周一
 * @Describe
 */
public class TestHbaseAPI_2 {
    public static void main(String[] args) throws IOException {
        //创建连接，会以当前类为key来创建这个类独有的线程
        HBaseUtils_old.makeHbaseConnect();

        HBaseUtils_old.insertData("holden:family","1002","info","sex","male");

        //关闭连接
        HBaseUtils_old.close();
    }
}
