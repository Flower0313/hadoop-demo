package com.atguigu.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-Mapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:57 - 周日
 * @Theme
 */

public class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable outKey = new IntWritable();
    private final static IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        //默认按key排序
        String[] lines = value.toString().split(" ");
        context.write(new IntWritable(Integer.parseInt(lines[1])), new Text(lines[0]));

    }
}
