package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName MapReduceDemo-CustomerProducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月01日17:20 - 周五
 * @Describe 不带回调函数的生产者
 */
public class Producer1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建kafka生产者的配置对象
        Properties properties = new Properties();

        // 2. 给kafka配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // 设置ack
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数,发送失败会重试，比如没收到ack，但可能造成消息重复，所以在接受者那边要做好幂等性处理
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);


        // 批次大小 默认16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 等待时间,默认1毫秒，要么到了16K发送数据，要么到了1毫秒发送数据
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        //使用自定义的分区器
        /*properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "com.atguigu.kafka.MyPartition");*/

        // RecordAccumulator本地缓冲区大小，默认32M，
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // key,value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 3. 创建kafka生产者对象
        Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 4. 调用send方法,发送消息
        //如果此时有两个分区，则会轮询发送，0,2,4在一个分区，1,3在一起分区
        //这里啥都没指定所以是轮询，假设2个分区，先随机一个1号分区，再0分区，继续轮询1,0,0,1...
        //当然也可以指定分区号存放
        for (int i = 0; i < 5; i++) {
            //异步方法
            kafkaProducer.send(new ProducerRecord<>("first1","kafka" + i));
            //同步方法，如果没有收到ack会发生阻塞
            //kafkaProducer.send(new ProducerRecord<>("holden","kafka" + i)).get();

            //使每次发送消息后睡眠2ms，让消息不在同一批次中
            Thread.sleep(2);
        }
        // 5. 关闭资源，如果不关闭，那么消费组收不到数据，因为数据设置要等1毫秒才发送，但是你send十次比1毫秒还短，既没有触及到批次大小，也没有达到发送时间，所以数据不会发。
        //在这里thread.sleep(1000)也能触及超过1毫秒发送的信号。
        //close()会刷新数据发送
        kafkaProducer.close();
    }
}
