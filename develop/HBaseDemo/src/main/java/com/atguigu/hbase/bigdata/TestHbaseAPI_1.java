package com.atguigu.hbase.bigdata;

import com.atguigu.hbase.utils.HBaseUtilDDL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MapReduceDemo-TestHbaseAPI_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月04日13:46 - 周一
 * @Describe 测试hbase的api
 */
public class TestHbaseAPI_1 {

    //0.创建配置对象，获取hbase的连接,他去找hadoop
    //方式二：
    //将linux上的hbase-site.xml拖到resources中，HBaseConfiguration.class.getClassLoader()会读取resource配置文件
    public static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException {

        isTableExist("student");


        //新增表
        //createTable(conn, "holden", "family", "info");

        //获取所有数据
        //getAllRows("student");

        //获取表中指定rowKey的数据
        //getTargetRows(conn,"holden:family","1001");

        //新增对象
        //addRowData(conn,"student","100","info","name","monkey");

        //获取指定的行和列
        //getRowQualifier("student", "1001", "info", "name");

        //删除多行数据
        //deleteMultiRow(conn,"holden:family","1001");
    }


    /**
     * 判断表是否在hbase中存在
     *
     * @param tableName 表名
     * @return boolean对象
     */
    public static boolean isTableExist(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        Admin admin = HBaseUtilDDL.connection.getAdmin();
        System.out.println(admin.tableExists(TableName.valueOf(tableName)));
        HBaseUtilDDL.close();

        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建指定命名空间下的表
     *
     * @param conn         连接对象
     * @param namespace    命名空间名称
     * @param tableName    表名
     * @param columnFamily 列族名
     */
    public static void createTable(Connection conn, String namespace, String tableName, String... columnFamily) throws
            MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = HBaseUtilDDL.connection.getAdmin();
        //判断命名空间是否存在
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            //创建表空间
            NamespaceDescriptor nd =
                    NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        }

        //判断表是否存在
        TableName tableName1 = TableName.valueOf(namespace + ":" + tableName);
        if (admin.tableExists(tableName1)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(tableName1);

            //给表添加协处理器
            descriptor.addCoprocessor("com.atguigu.hbase.mr.mr3.InsertAtguiguStudentCoprocesser");
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
        HBaseUtilDDL.close();
    }

    /**
     * @param conn      连接对象
     * @param namespace 命名空间名称
     * @param tableName 表名
     */
    //删除表
    public static void dropTable(Connection conn, String namespace, String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        Admin admin = HBaseUtilDDL.connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(namespace + ":" + tableName));
            admin.deleteTable(TableName.valueOf(namespace + ":" + tableName));
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    /**
     * @param conn
     * @param tableName 表名
     * @param rowKey    行主键
     */
    //获取表中的行数据
    public static void getTargetRows(Connection conn, String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        //1001是RowKey，需要字符编码
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        boolean empty = result.isEmpty();
        if (!empty) {
            System.out.println("已查询到下列数据！");
            for (Cell cell : result.rawCells()) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }


        } else {
            System.out.println("没有数据。。。");
        }
    }

    /**
     * @param conn         连接对象
     * @param tableName    表名
     * @param rowKey       主键
     * @param columnFamily 列族名
     * @param column       列名
     * @param value        列值
     */
    public static void addRowData(Connection conn, String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException {
        //创建HTable对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("新增成功...");
    }

    public static void getAllRows(String tableName) throws IOException {
        Table table = HBaseUtilDDL.connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("------");
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HBaseUtilDDL.close();
    }

    /**
     * 获取指定行指定列
     *
     * @param conn      链接对象
     * @param tableName 表名
     * @param rowKey    主键名
     * @param family    列族名
     * @param qualifier 列名
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException {
        Table table = HBaseUtilDDL.connection.getTable(TableName.valueOf(tableName));
        //rowKey是以字节数组方式存储的，不能直接以String的方式去查
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("------");
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        HBaseUtilDDL.close();
    }

    //删除多行数据
    public static void deleteMultiRow(Connection conn, String tableName, String... rows) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    /**
     * 获取某一行数据
     *
     * @param tableName 表名
     * @param rowKey    行主键
     * @throws IOException
     */
    public static void getRow(Connection conn, String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }


}
//rfect diary
