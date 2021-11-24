package com.atguigu.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName KafkaDemo-Consumer2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日13:53 - 周三
 * @Describe 深入理解kafka
 */
public class Producer2 {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");


        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(pros);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("kafkaTest", "hello,flower");

        producer.send(record);

        producer.close();


    }
}
