package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-WebLogMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月10日8:20 - 周五
 * @Theme
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        boolean result = parseLog(line, context);
        //若不为true直接退出
        if (!result) {
            return;
        }
        context.write(value, NullWritable.get());

    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        //按空格来截取
       /* String[] split = line.split("\\s+");
        if (split.length < 11) {
            return true;
        } else {
            return false;
        }*/
        if(line.contains("abiqiatguigu")){
            return true;
        }else{
            return false;
        }
    }
}
