package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:58 - 周日
 * @Theme
 *
 * KEYIN:表示reduce阶段输入kv中k类型对应着map输出的key
 * VALUEIN:表示reduce阶段输入kv类型中v类型，对应may输出的value，对应着单词次数1
 * KEYOUT:本需求中还是单词
 * VALUEOUT:也是次数，看自己的需求
 * todo Q:当map的所有输出数据来到reduce之后，该如何调用reduce方法进行处理呢？
 * <hello,1><hello,1><hadoop,1><hello,1>
 *    1.排序，根据key的字典序排序a-z，
 *    2.分组，key相同的分为一组
 *              <hadoop,1>
 *              <hello,1><hello,1><hello,1>
 *    3.分组之后同一组组成新的kv键值对，调用一次reduce方法。
 *              新key:改组共同的key
 *              新value:该组所有value组成的迭代器
 *              <hadoop,Iterable[1]>
 *              <hello,Iterable[1,1,1]>
 *    4.再进行循环累加，实现自己的业务逻辑
 */


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum;
    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("我是reduce");
        // 1 累加求和
        sum = 0;
        //<hello,Iterable[1,1,1]>
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2 输出
        v.set(sum);

        //在Task类中调用了WrappedReducer.getReducerContext()，这个方法是给WrappedReducer中的内部类Context中的
        //reduceContext成员变量赋值ReduceContextImpl，所以write最后调用到ReduceContextImpl的write，但是ReduceContextImpl
        //中的write没有重写，所以还是调用它父类的TaskInputOutputContextImpl中write()，而它中的write是调用
        //RecordWriter的write()
        context.write(key,v);
    }
}
