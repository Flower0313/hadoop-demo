package com.atguigu.mapreduce.wordcount;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ClassName MapReduceDemo-WordCountDriverV3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月10日10:25 - 周五
 * @Theme reduce输出端形成压缩文件
 */
public class CombineTextInputDemo {
    public static void main(String[] args) throws Exception {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(CombineTextInputDemo.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);


        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件切片为4m，设置切片就不会按文件来单独切片了
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //CombineTextInputFormat.setMaxInputSplitSize(job, 33554432);

        // 6 设置输入和输出路径
        //注意这个输出路径下的文件必须不能存在
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("CombineTextInputDemo")));

        //设置reduce压缩开启
        //FileOutputFormat.setCompressOutput(job, true);
        //设置reduce输出端压缩的方式
        //FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // 7 提交job,此方法蕴含job.submit()方法
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
