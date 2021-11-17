/*
package com.atguigu.hbase.mr.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

*/
/**
 * @ClassName MapReduceDemo-ScanDataMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日10:40 - 周二
 * @Describe
 *//*

public class ScanDataMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        //查询数据
        Put put = new Put(key.get());

        for (Cell cell : value.rawCells()) {
            put.addColumn(
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    CellUtil.cloneValue(cell)
            );
        }
        //将put给reduce
        context.write(key, put);
    }
}
*/
