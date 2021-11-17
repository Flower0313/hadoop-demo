package com.atguigu.mapreduce.mapjoincompara;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @ClassName MapReduceDemo-MapJoinMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日15:43 - 周四
 * @Theme
 */
    public class MapJoinMapper extends Mapper<LongWritable, Text, IntWritable, TableBean2> {
    HashMap<String, String> pdMap = new HashMap<>();
    private IntWritable outK = new IntWritable();
    private TableBean2 outV = new TableBean2();

    /**
     * 每次maptask任务开启前有且只会执行一次，一个文件对应一个切片，一个切片对应一个MapTask
     * 没有reduceTask阶段那么排序也用不到了
     *
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, TableBean2>.Context context)
            throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fileSystem.open(new Path(cacheFiles[0]));

        //从流中读数据，因为这个流的readLine()可以一行一行读取
        //InputStreamReader是Reader的子类，是从字节流到字符流的桥梁。它读取字节，它接收InputStream的子类
        //并使用指定的字符集将其解码为字符。它的字符集可以由名称指定，也可以接受平台的默认字符集。
        //BufferedReader接收Reader的子类
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] split = line.split("\\s+");
            pdMap.put(split[0], split[1]);//将编号对应的名字放入HashMap中
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, TableBean2>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\\s+");
        outK.set(Integer.parseInt(split[2]));
        outV.setId(split[0]);
        outV.setPname(pdMap.get(split[1]));
        outV.setPid("");
        outV.setFlag("");
        outV.setAmount(Integer.parseInt(split[2]));
        context.write(outK, outV);
    }
}
