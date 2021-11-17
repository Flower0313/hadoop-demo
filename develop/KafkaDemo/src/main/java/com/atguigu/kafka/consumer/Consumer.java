package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName MapReduceDemo-CustomConsumer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月01日19:53 - 周五
 * @Describe 自动提交
 */
public class Consumer {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();

        // 2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //自动提交offset，1秒提交一次。消费到哪了
        //如果这里为false，那么不会自动提交消费到哪了，每次都是从头开始读取
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //从头读取的前提是,那就是对于同一个groupid的消费者,如果这个topic某个分区有已经提交的offset,
        // 那么无论是把auto.offset.reset=earliest还是latest,都将失效,消费者会从已经提交的offset开始消费.
        //所以想让这个生效就要用别的消费者
        //重置消费者offset，当你换消费组了或者消费组已经过了7天挂了，才会生效就从头开始读取，默认latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //如果一个消费者两次poll的时间间隔超过了10秒，kafka会认为其能力过差，会将其踢出消费组，将分区分配给别人。
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,10*1000);
        // 配置消费者组，不分配会被随机
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flume2");
        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 注册主题,选择消费主题，可以定义多个主题，所以是List
        ArrayList<String> strings = new ArrayList<>();
        strings.add("topic_log");
        consumer.subscribe(strings);
        //consumer.subscribe(Arrays.asList("holden"));

        // 循环拉取数据打印，用kill关掉
        while (true) {
            //默认一次拉取500条消息
            //这里配置1毫秒的意思是，无论你poll多少消息，只要到达1毫秒就结束，有可能你这次啥都没拉到，有可能拉完了，所以要while,循环拉
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.topic()+","+consumerRecord.partition()+","+consumerRecord.offset());
            }
        }

    }

}
