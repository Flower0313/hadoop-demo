package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @ClassName KafkaDemo-Consumer2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日14:07 - 周三
 * @Describe
 */
public class Consumer2 {
    public static void main(String[] args) {
        Properties pros = new Properties();

        // 配置序列化 必须
        pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //设置消费组
        pros.put(ConsumerConfig.GROUP_ID_CONFIG,"group.demo");

        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(pros);

        consumer.subscribe(Collections.singletonList("kafkaTest"));

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
