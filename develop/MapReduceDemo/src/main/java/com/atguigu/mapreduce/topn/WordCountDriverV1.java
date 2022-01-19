package com.atguigu.mapreduce.topn;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WordCountDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日23:58 - 周日
 * @Theme com.atguigu.mapreduce.wordcount.WordCountDriverV1
 */
public class WordCountDriverV1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        //Configuration conf = new Configuration();
        Job job = Job.getInstance();

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriverV1.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        //注意这个输出路径下的文件必须不能存在
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\topn.txt"));

        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("topn")));

        // 7 提交job,此方法蕴含job.submit()方法,参数为true的意思的开启监视并打印错误日志
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
