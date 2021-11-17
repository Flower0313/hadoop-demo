package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-LogRecordWriter
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月08日17:00 - 周三
 * @Theme
 */
//由前面可知，最后reduce.write()调用的就是RecordWriter中的write方法，只要我们重写RecorWriter类的write方法,然后是ReduceTask中的
    //createReduceContext()的这个方法调用的后面的构造方法
    //ReduceContextImpl调用父类TaskInputOutputContextImpl的构造方法会把我们这个类给初始化，接着会调这个类中的write()重写方法
    //最终是在ReduceTask的内部类NewTrackingRecordWriter的构造方法中的real成员变量赋值，
    //因为LogOutputFormat继承了FileOutputFormat并实现了getRecordWriter()，然后real变量收到的其实是LogRecordWriter实现类
    //这样就通了，
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;
    private static final String TARGETNAME = "atguigu";


    public LogRecordWriter(TaskAttemptContext job) {
        //创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            atguiguOut = fs.create(new Path("T:\\ShangGuiGu\\hadoop\\output\\logs\\atguigu.log"));

            otherOut = fs.create(new Path("T:\\ShangGuiGu\\hadoop\\output\\logs\\other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //这个write方法真正的写，这个方法是在LogReducer中reduce后面执行的，每个reduce都会调用
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        if (line.contains(TARGETNAME)) {
            atguiguOut.writeBytes(line+"\n");
        } else {
            otherOut.writeBytes(line+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
