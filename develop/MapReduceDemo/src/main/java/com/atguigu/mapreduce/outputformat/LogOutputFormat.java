package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-LogOutputFormat
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月08日16:58 - 周三
 * @Theme
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    //reduce输出的kv就传到了这里,这里的自定义LogOutputFormat会在ReduceTask类中被初始化
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        //将LogRecordWriter需要的job带过去
        LogRecordWriter lrw = new LogRecordWriter(job);
        return lrw;
    }
}
