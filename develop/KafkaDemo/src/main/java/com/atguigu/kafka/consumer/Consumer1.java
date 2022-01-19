package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaOffset2");

        //不排除内部offset，不然看不到__consumer_offsets
        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 注册主题topic
        ArrayList<String> strings = new ArrayList<>();
        strings.add("test");
        consumer.subscribe(strings);

        //Attention 递增的序号
        AtomicInteger offsetFlag = new AtomicInteger();

        // 拉取数据打印
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            //Attention 同步提交offset,当前while会阻塞直到offset提交成功
            //consumer.commitSync();

            System.out.println("=====");
            //Attention 异步提交offset,走到这里时单独有个线程去提交,while()继续执行
            /*
             * Explain
             *  我们可以设置一个递增的序号来维护异步提交的顺序，每次位移提交之后就增加序号相对应的值。在遇到位移提交失败需要重试的时候，
             *  可以检查所提交的位移和序号的值的大小，如果前者小于后者，则说明有更大的位移已经提交了，不需要再进行本次重试；
             *  如果两者相同，则说明可以进行重试提交。除非程序编码错误，否则不会出现前者大于后者的情况
             *  因为offset也是逐个自增的，可以查看LOG-END-OFFSET的值
             *
             * */
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    //如果出现异常
                    if (exception != null) {
                        System.err.println("提交失败" + offsets);
                    } else {

                        //有消息提交才能自增


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
                            //String topic = topicPartition.topic();
                            //System.out.printf("----提交的offset = %s, 该 partition = %s ,以及topic = %s,自增序号 = %s----\n",
                            //offset, partition, topic, offsetFlag);
                        }
                    }
                }
            });
        }
    }
}
