package com.atguigu.mapreduce.reducejoin;

import com.atguigu.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TableDriver
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日14:34 - 周四
 * @Theme
 */
public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.声明conf和job
        Job job = Job.getInstance(new Configuration());

        //2.关联类
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        //3.设置mapper输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //4.设置reducer输出数据类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("T:\\ShangGuiGu\\hadoop\\input\\inputtable"));
        FileOutputFormat.setOutputPath(job, new Path(FileUtils.getProFileName("redecejoin")));

        //6.开启驱动并打印日志
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
