package com.atguigu.mapreduce.partitionandcompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName MapReduceDemo-ProvincePartitioner
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月07日20:27 - 周二
 * @Theme
 */

//这里的kv就是对应的map出来kv,Partitioner也是Mapper过渡到Reducer的中间件
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        //text 是手机号
        String phone = text.toString();
        String prePhoneThree = phone.substring(0, 3);
        //手机号的前3位不同相对应的分区号也不同
        int partition;
        if ("136".equals(prePhoneThree)) {
            partition = 0;
        } else if ("137".equals(prePhoneThree)) {
            partition = 1;
        } else if ("138".equals(prePhoneThree)) {
            partition = 2;
        } else if ("139".equals(prePhoneThree)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
