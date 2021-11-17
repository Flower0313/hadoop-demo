package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @ClassName MapReduceDemo-CustomConsumer1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月01日19:54 - 周五
 * @Describe 手动提交
 */
public class Consumer1 {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();

        // 2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");


        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组 必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        //不排除内部offset，不然看不到__consumer_offsets
        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 注册主题topic
        ArrayList<String> strings = new ArrayList<>();
        strings.add("first1");
        consumer.subscribe(strings);

        // 拉取数据打印
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            //同步提交offset
            //consumer.commitSync();

            System.out.println("=====");
            //异步提交offset
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    //如果出现异常
                    if (exception != null) {
                        System.err.println("提交失败" + offsets);
                    } else {
                        Set<TopicPartition> topicPartitions = offsets.keySet();
                        //该方法可以得到我们消费的的消息所处的topic partition有哪些
                        //遍历我们消费的topic 以及parition 元数据
                        for (TopicPartition topicPartition : topicPartitions) {
                            //每个topic的每个parition的消费到的offset
                            OffsetAndMetadata offsetAndMetadata = offsets.get(topicPartition);
                            //获取提交的offset值
                            long offset = offsetAndMetadata.offset();
                            //获取该parttion的值
                            int partition = topicPartition.partition();
                            //获取该toipic的值
                            String topic = topicPartition.topic();
                            System.out.printf("----提交的offset = %s, 该 partition = %s ,以及topic = %s---------\n",
                                    offset, partition, topic);
                        }
                    }
                }
            });
        }
    }
}
