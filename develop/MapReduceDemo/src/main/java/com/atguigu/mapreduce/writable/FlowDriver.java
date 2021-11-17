package com.atguigu.mapreduce.writable;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @ClassName MapReduceDemo-FlowDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月06日22:55 - 周一
 * @Theme
 */
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(FlowDriver .class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text .class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 设置输入和输出路径
        //注意这个输出路径下的文件必须不能存在
        //测试环境
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputflow\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("writable")));

        //生产环境
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
