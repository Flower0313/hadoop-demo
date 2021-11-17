package com.atguigu.mapreduce.etl;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ClassName MapReduceDemo-WebLogDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月10日8:25 - 周五
 * @Theme
 */
public class WebLogDriver {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 加载jar包
        job.setJarByClass(WebLogDriver.class);
        // 3 关联map
        job.setMapperClass(WebLogMapper.class);
        // 4 设置mapper输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置reducetask个数为0
        job.setNumReduceTasks(0);
        // 6设置输入和输出路径
        //FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputlog"));
        FileInputFormat.setInputPaths(job,new Path("T:\\ShangGuiGu\\hadoop\\input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("etl")));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
