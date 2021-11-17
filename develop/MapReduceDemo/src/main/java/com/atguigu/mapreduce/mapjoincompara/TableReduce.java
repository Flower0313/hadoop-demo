package com.atguigu.mapreduce.mapjoincompara;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TableReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月16日23:25 - 周四
 * @Describe
 */
public class TableReduce extends Reducer<IntWritable, TableBean2, TableBean2, NullWritable> {
    //只能实现按价格升序，如果要倒序，则需要自定义Bean类compareTo
    @Override
    protected void reduce(IntWritable key, Iterable<TableBean2> values, Reducer<IntWritable, TableBean2, TableBean2, NullWritable>.Context context) throws IOException, InterruptedException {
        for (TableBean2 value : values) {
            context.write(value,NullWritable.get());
        }
    }
}
