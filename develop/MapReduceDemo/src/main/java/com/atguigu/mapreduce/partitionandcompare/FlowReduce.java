package com.atguigu.mapreduce.partitionandcompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-FlowReduce
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月06日22:40 - 周一
 * @Theme 这里的FloReduce的输入还是FlowMapper的输出，但这里的输出就是看自己的需求了，
 * 此时的需求是，输出相同key累加后的上行流量、下载流量、总和流量
 */
public class FlowReduce extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void setup(Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        System.out.println("我是reducers!");
    }

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //如果总流量相同的手机号只会出现一个，需求是每个手机号都输出
        //Q:这里的对象类型如何判断相等的？
        //A:这里判断对象相等的方式是，判断每个字段是否相等，比如FlowBean中，则判断它序列化的字段upFlow和downFlow和SumFlow的equals方法
        System.out.println("");
        for (Text value : values) {
            System.out.print("====================="+value+"=====================");
            context.write(value, key);
        }
    }
}
