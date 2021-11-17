package com.atguigu.mapreduce.writableComparable;

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
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //如果总流量相同的手机号只会出现一个，需求是每个手机号都输出
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
