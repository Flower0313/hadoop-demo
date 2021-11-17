package com.atguigu.mapreduce.partitionandcompare;

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
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private  Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();

        //2.进行切割
        String[] split = line.split("\\s+");
        /*
        1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        2	13846544121	192.196.100.2			264	0	200
        * */
        //3.封装
        outV.set(split[0]);//手机号
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        //4.写出
        context.write(outK,outV);

    }
}
