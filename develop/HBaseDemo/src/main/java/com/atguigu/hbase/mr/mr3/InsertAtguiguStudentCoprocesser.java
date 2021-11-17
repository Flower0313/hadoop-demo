/*
package com.atguigu.hbase.mr.mr3;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

*/
/**
 * @ClassName MapReduceDemo-coprocesser
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日23:44 - 周二
 * @Describe 协处理器(HBase)
 *//*

public class InsertAtguiguStudentCoprocesser extends BaseRegionObserver {
    //在put之后执行
    //增加student的数据同时增加atguigu:user中数据
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("atguigu:user"));
        //这样可能会导致死循环，因为这里也是put，会陷入死循环，所以可以只给student加协处理器
        table.put(put);

        table.close();
    }
}
















*/
