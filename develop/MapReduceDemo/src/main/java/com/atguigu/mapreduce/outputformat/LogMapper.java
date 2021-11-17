package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-LogMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月08日16:39 - 周三
 * @Theme
 */

//输出因为是只有一列数据，所以直接value为null就行
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    /**
     * 输入数据：
     * http://www.baidu.com
     * http://www.google.com
     * http://cn.bing.com
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}
