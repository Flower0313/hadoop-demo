package com.atguigu.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName MapReduceDemo-MyPartition
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月02日10:03 - 周六
 * @Describe
 */
public class MyPartition implements Partitioner {
    /**
     * 返回信息对应的分区
     * @param topic         主题
     * @param key           消息的key
     * @param keyBytes      消息的key序列化后的字节数组，已经不是String类型了
     * @param value         消息的value
     * @param valueBytes    消息的value序列化后的字节数组
     * @param cluster       集群元数据可以查看分区信息
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取消息
        String msgValue = value.toString();
        // 创建partition
        int partition;
        // 判断消息是否包含atguigu,包含atguigu的进入0分区，否则进入1分区
        if (msgValue.contains("atguigu")){
            partition = 0;
        }else {
            partition = 1;
        }
        // 返回分区号
        return partition;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 配置方法
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

}
