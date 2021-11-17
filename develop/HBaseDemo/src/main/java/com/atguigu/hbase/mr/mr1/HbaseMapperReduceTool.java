/*
package com.atguigu.hbase.mr.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.client.Put;


*/
/**
 * @ClassName MapReduceDemo-HbaseMapperReduceTool
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日0:53 - 周二
 * @Describe
 *//*

public class HbaseMapperReduceTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //不传conf，自己会new Conf
        Job job = Job.getInstance();
        //传这个是因为后面会根据这个类名去找包含此类的jar包
        job.setJarByClass(HbaseMapperReduceTool.class);

        //mapper
        //hbase ==> hbase
        //这个方法中封装了以前的setMapperClass(类名)的方法。
        TableMapReduceUtil.initTableMapperJob(
                "holden:family",
                new Scan(),
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "holden:user",
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
