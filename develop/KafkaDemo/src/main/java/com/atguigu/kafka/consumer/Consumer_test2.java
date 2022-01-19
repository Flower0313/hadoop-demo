package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @ClassName MapReduceDemo-Consumer_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月10日20:14 - 周一
 * @Describe
 */
public class Consumer_test2 {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();

        // 2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //自动提交offset，1秒提交一次。消费到哪了
        //如果这里为false，那么不会自动提交消费到哪了，每次都是从头开始读取
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Attention 配置序列化,因为kafka是网络传输,所以必须得key和value进行序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //如果一个消费者两次poll的时间间隔超过了10秒，kafka会认为其能力过差，会将其踢出消费组，将分区分配给别人。
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10 * 1000);
        // 配置消费者组，不分配会被随机
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flower");
        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 注册主题,选择消费主题，可以定义多个主题，所以是List
        ArrayList<String> strings = new ArrayList<>();
        strings.add("test");
        consumer.subscribe(strings);

        // 循环拉取数据打印，用kill关掉
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            //将拉取到的消息循环打印
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("分区=>" + consumerRecord.partition() +
                        "\tkey==>" + consumerRecord.key() +
                        "\tvalue==>" + consumerRecord.value());
            }
        }
    }
}
