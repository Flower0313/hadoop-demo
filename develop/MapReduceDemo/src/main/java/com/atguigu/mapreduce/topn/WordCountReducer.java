package com.atguigu.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:58 - 周日
 * @Theme KEYIN:表示reduce阶段输入kv中k类型对应着map输出的key
 * VALUEIN:表示reduce阶段输入kv类型中v类型，对应may输出的value，对应着单词次数1
 * KEYOUT:本需求中还是单词
 * VALUEOUT:也是次数，看自己的需求
 */


public class WordCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private int topn;
    private int index;

    @Override
    protected void setup(Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        index = 0;
        topn = 3;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if (!(index++ > topn)) {
                context.write(new Text(value), key);
            }
        }
    }
}
