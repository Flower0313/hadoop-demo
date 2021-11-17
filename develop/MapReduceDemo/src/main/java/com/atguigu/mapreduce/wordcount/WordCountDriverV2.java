package com.atguigu.mapreduce.wordcount;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountDriverV2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月08日19:07 - 周三
 * @Theme 采用meduce输出端压缩文件
 */

public class WordCountDriverV2 {
    public static void main(String[] args) throws Exception {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();

        //开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


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

        // 6 设置输入和输出路径
        //注意这个输出路径下的文件必须不能存在
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("WordCountDriverV2")));

        // 7 提交job,此方法蕴含job.submit()方法
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
