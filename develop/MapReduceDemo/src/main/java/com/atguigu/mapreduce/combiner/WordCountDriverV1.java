package com.atguigu.mapreduce.combiner;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:58 - 周日
 * @Theme
 */
public class WordCountDriverV1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriverV1.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置文件切片为4m
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Combiner的第一种写法,用自己的Combiner类
        job.setCombinerClass(WordCountCombiner.class);
        //Combiner的第二种写法，用Reducer类，因为reduce()代码是一样的
        //job.setCombinerClass(WordCountReducer.class);

        // 6 设置输入和输出路径
        //注意这个输出路径下的文件必须不能存在
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("combiner")));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
