package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @ClassName KafkaDemo-Consumer2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日14:07 - 周三
 * @Describe
 */
public class Consumer2 {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();

        // 2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");


        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组 必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaOffset");

        //不排除内部offset，不然看不到__consumer_offsets
        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 注册主题topic
        consumer.subscribe(Collections.singletonList("test"));
        //https://weread.qq.com/web/reader/e9a32a0071848698e9a39b8k70e32fb021170efdf2eca12
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {//若不为0，则证明分配到了分区
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        //设置偏移量
        for (TopicPartition topicPartition : assignment) {
            System.out.println(topicPartition);
            consumer.seek(topicPartition, 59);
        }

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(3));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }
    }
}
