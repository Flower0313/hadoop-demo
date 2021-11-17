package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-Mapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:57 - 周日
 * @Theme KEYIN:表示map阶段输入Kv中k类型，起始偏移量是LongWritable
 * VALUEIN:表示map阶段输入Kv中的v类型,一行内容
 * 读数据是一行一行地去读，返回kv键值对
 * k：每一行的起始位置的偏移量 通常无意义
 * v：这一行的文本内容
 * KEYOUT:表示map阶段输出Kv中k，跟业务相关，本需求输出的是单词，因此是Text
 * VALUEOUT: 同上，本需求输出的是单词次数1，因此是IntWritable，所以outValue是int类型
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outKey = new Text();
    private final static IntWritable outValue = new IntWritable(1);

    //setup只会执行一次
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("我是setup");
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //1.获取一行
        //value是hello hello，key是偏移量
        System.out.println("偏移量：" + key);
        String line = value.toString();

        //2.切割字符串
        //hello
        //hello
        //按空白切割
        String[] words = line.split("\\s+");

        //3.循环写出
        //<hello,1>与<hello,1>
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }

    }
}
