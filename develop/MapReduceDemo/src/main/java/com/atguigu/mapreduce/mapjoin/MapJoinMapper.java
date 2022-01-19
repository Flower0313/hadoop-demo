package com.atguigu.mapreduce.mapjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MapReduceDemo-MapJoinMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月29日17:51 - 周三
 * @Describe
 */
public class MapJoinMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    private Map<String,String> pdMap = new HashMap<>();
    private Text text = new Text();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);


    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
