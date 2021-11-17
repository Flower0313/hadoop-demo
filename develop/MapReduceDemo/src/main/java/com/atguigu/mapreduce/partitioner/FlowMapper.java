package com.atguigu.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-FlowMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月06日22:11 - 周一
 * @Theme
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1.获取一行信息
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();

        //2.切割
        //[1,13736230513,192.196.100.1,www.atguigu.com,2481,24681,200]
        String[] words = line.split("\\s+");

        //3.抓取自己想要的数据
        String phoneNumber = words[1];
        Long upFlow = Long.valueOf(words[words.length - 3]);
        Long downFlow = Long.valueOf(words[words.length - 2]);

        //4.封装一个FlowBean作为手机号对应的Value值,每次新来的对象会覆盖老对象的数据，所以new一个FlowBean就行
        outK.set(phoneNumber);
        outV.setUpFlow(upFlow);
        outV.setDownFlow(downFlow);
        outV.getSumFlow(upFlow, downFlow);

        //5.写出，传过去的是个FlowBean对象
        context.write(outK, outV);
    }
}
