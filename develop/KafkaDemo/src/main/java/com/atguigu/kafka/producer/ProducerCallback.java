package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @ClassName MapReduceDemo-CustomProducerCallback
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月01日17:27 - 周五
 * @Describe
 */
public class ProducerCallback {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建kafka生产者的配置对象
        Properties properties = new Properties();

        // 2. 给kafka配置对象添加配置信息
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // key,value序列化(必须)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 设置ack
        properties.put("acks", "all");
        // 重试次数
        properties.put("retries", 3);
        // 批次大小 默认16K
        properties.put("batch.size", 16384);
        // 等待时间
        properties.put("linger.ms", 1);
        // RecordAccumulator缓冲区大小 默认32M，只有满足上面两个条件才会send消息
        properties.put("buffer.memory", 33554432);

        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 4. 调用send方法,发送消息
        for (int i = 0; i < 50; i++) {
            // 添加回调
            // 该方法在Producer收到ack时调用，为异步调用
            kafkaProducer.send(new ProducerRecord<>("first1", "second" + i),
                    (metadata, exception) -> {
                if (exception == null) {
                    // 没有异常,输出信息到控制台，这个和send是异步的，如果不在主线程加sleep是看不到这条消息的，因为send发完就结束这个进程了
                    System.out.println(metadata);
                    //System.out.println("partition:"+metadata.partition() +"，offset:"+ metadata.offset());
                } else {
                    // 出现异常打印
                    exception.printStackTrace();
                }
            });

            //使每次发送消息后睡眠2ms，让消息不在同一批次中
            Thread.sleep(2);
        }
        // 5. 关闭资源
        kafkaProducer.close();

    }
}
