package com.atguigu.mapreduce.mapjoincompara;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @ClassName MapReduceDemo-TableDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日14:34 - 周四
 * @Theme
 */
public class MapJoinDriver {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Job job = Job.getInstance(new Configuration());
        // 2 设置加载jar包路径
        job.setJarByClass(MapJoinDriver.class);
        // 3 关联mapper
        job.setMapperClass(MapJoinMapper.class);
        job.setReducerClass(TableReduce.class);

        // 4 设置Map输出KV类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TableBean2.class);

        // 5 设置最终输出KV类型
        job.setOutputKeyClass(TableBean2.class);
        job.setOutputValueClass(NullWritable.class);

        // 加载缓存数据,加载小的文件，加载到了DistributedCache
        job.addCacheFile(new URI("file:///T:/ShangGuiGu/hadoop/input/inputtable/pd.txt"));
        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputtable2\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("MapJoinDriver")));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
