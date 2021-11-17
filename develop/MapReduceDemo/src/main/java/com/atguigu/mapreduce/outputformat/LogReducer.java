package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-LogReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月08日16:46 - 周三
 * @Theme
 */
public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //在这里要考虑两条相同数据的情况，如有key相同的，需要将value循环
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
