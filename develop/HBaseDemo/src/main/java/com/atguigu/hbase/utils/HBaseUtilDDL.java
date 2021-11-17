package com.atguigu.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-HBaseUtilDDL
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月09日16:49 - 周六
 * @Describe
 */
public class HBaseUtilDDL {

    // 声明静态属性
    public static Connection connection;

    // 初始化单例连接
    static {
        try {
            System.out.println("connection被初始化...");
            //1.获取hbase连接对象
            //classpath:hbase-default.xml,hbase-site.xml，这里面指明了去哪里加载
            //加不加conf无所谓，不传参会自动HBaseConfiguration.create()
            //Configuration conf = HBaseConfiguration.create();
            Configuration conf = new Configuration();

            //2.给配置类添加配置
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            //conf.set("hbase.zookeeper.property.clientPort", "2181");
            //3.创建连接,如果使用空参的话也会自动调用HBaseConfiguration.create()

            connection = ConnectionFactory.createConnection(conf);
            System.out.println(connection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        connection.close();
    }

    public static void main(String[] args) throws IOException {

        createTable("holden", "flower", 2, "info");
        //3.关闭资源
        close();
    }

    //判断表是否存在
    public static boolean isTableExist(String namespace, String tableName) throws IOException {
        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.判断表是否存在
        boolean b = admin.tableExists(TableName.valueOf(namespace, tableName));
        admin.close();
        return b;
    }

    //创建表
    public static boolean createTable(String namespace, String tableName, Integer version, String... familyNames) throws IOException {
        if (familyNames.length == 0) {
            return false;
        }

        if (isTableExist(namespace, tableName)) {
            System.out.println("表已经存在！");
            return false;
        } else {
            Admin admin = connection.getAdmin();

            //背后也会创建ModifyableTableDescriptor()方法
            //将ModifyableTableDescriptor desc赋初值，等最后一起build()执行
            TableDescriptorBuilder builder =
                    TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

            for (String familyName : familyNames) {
                // 获取单个列族的descriptor，因为是私有构造方法所以只能用它提供的newBuilder
                ColumnFamilyDescriptorBuilder cfdb =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));

                // 添加版本,建造者模式，然后将这些设置的参数都加入到了valuesMap中
                //这里的desc是ModifyableColumnFamilyDescriptor类
                cfdb.setMaxVersions(version);
                // 将单个列族的descriptor添加到builder中
                //setColumnFamily需要ColumnFamilyDescriptor,而build()后的ModifyableColumnFamilyDescriptor就是其子类
                builder.setColumnFamily(cfdb.build());
            }
            //build()是为执行之前赋好值的ModifyableTableDescriptor desc
            //ModifyableTableDescriptor是TableDescriptor的实现类
            admin.createTable(builder.build());
            admin.close();
            System.out.println("创建成功！");
            return true;
        }
    }

    //修改表
    public static void modifyTable(String nameSpace, String tableName, String familyName, int version) throws IOException {
        // 提前判断表格存在
        if (!isTableExist(nameSpace,tableName)) {
            System.out.println("修改的表格不存在");
            return;
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2.获取原先的描述
        TableDescriptor descriptor =
                admin.getDescriptor(TableName.valueOf(nameSpace, tableName));
        // 3.获取原先描述的builder
        //若在newBuilder()的方法中放tableName会直接创建一个表，若填写的是descriptor则是修改表
        TableDescriptorBuilder tdb =
                TableDescriptorBuilder.newBuilder(descriptor);

        // 4.获取到原先的列族并进行修改
        ColumnFamilyDescriptorBuilder cfdb =
                ColumnFamilyDescriptorBuilder.
                        newBuilder(descriptor.getColumnFamily(Bytes.toBytes(familyName))).
                        setMaxVersions(version);
        // 5.传递列族描述
        tdb.modifyColumnFamily(cfdb.build());

        // 6.传递表格描述
        admin.modifyTable(tdb.build());
        // 7.关闭资源
        admin.close();
    }
}
