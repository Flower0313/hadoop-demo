package com.atguigu.mapreduce.writable;

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
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;

        //1.遍历集合累加值
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        //2.封装，目前输出的手机号key有，但是输出的总体结构我们还没有，所以要封装一个
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow(totalUp , totalDown);

        //3.写出最终的需求方案，输出的格式由toString()指定
        context.write(key,outV);

    }
}
