/*
package com.atguigu.hbase.mr.mr2;

import com.atguigu.hbase.mr.mr1.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

*/
/**
 * @ClassName MapReduceDemo-File2TableTool
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日17:44 - 周二
 * @Describe
 *//*

public class File2TableTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(File2TableTool.class);

        //在hdfs哪个地方读取
        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop102:8020/opt/module/hbase/student.csv"));

        //hdfs ==> hbase
        //mapper，先在是从文件到hbase，所以不能用hbase的工具了
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "student",
                InsertDataReducer.class,
                job
        );
        //执行作业
        boolean flag = job.waitForCompletion(true);
        return flag ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
*/
