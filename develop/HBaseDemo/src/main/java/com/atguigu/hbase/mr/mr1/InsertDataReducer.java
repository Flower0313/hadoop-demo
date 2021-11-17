/*
package com.atguigu.hbase.mr.mr1;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


*/
/**
 * @ClassName MapReduceDemo-InsertDataReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日11:21 - 周二
 * @Describe
 * @param ImmutableBytesWritable 是主键
 * @param Put 放入的对象
 *//*

//为啥传三个呢？因为有一个是固定的Mutation，Delete和Put都是继承它
public class InsertDataReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        //运行reducer,考虑一个rowkey会对应多个列
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
*/
