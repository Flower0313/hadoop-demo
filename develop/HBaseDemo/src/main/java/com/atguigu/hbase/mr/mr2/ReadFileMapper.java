/*
package com.atguigu.hbase.mr.mr2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;

import java.io.IOException;


*/
/**
 * @ClassName MapReduceDemo-ReadFileMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日18:03 - 周二
 * @Describe
 *//*

public class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        //1001,a
        String[] lines = value.toString().split(",");
        String rowKey = lines[0];
        String name = lines[1];

        Put put = new Put(Bytes.toBytes(rowKey));
        //这是加到列族info中列名为name，如果有多个列名，则要addColumn多行
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
    }
}
*/
