package com.atguigu.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-HBaseUtils
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月04日22:23 - 周一
 * @Describe
 */
public class HBaseUtils_old {
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    private static Connection conn = null;

    public static void makeHbaseConnect() throws IOException {
        //get()和set()底层是Entry[]，key就是你调用这个方法的类，所以是类独有线程。
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 增加数据
     *
     * @param rowkey
     * @param family 列族名
     * @param column 列名
     * @param value  列值
     */
    public static void insertData(String tablename, String rowkey, String family, String column, String value) throws IOException {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tablename));

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);

    }


}
