package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountCombiner
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月07日22:48 - 周二
 * @Theme
 */
//Combiner的输入kv是Mapper传出来的kv,它其实是在Map阶段的
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
   //Combiner是处理自己的MapTask中的数据，不会处理别的MapTask中的。
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key, outV);
    }
}
