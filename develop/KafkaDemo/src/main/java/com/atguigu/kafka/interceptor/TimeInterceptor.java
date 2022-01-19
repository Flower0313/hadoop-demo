package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName MapReduceDemo-TimeInterceptor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月31日1:39 - 周五
 * @Describe 自定义拦截器，在消息前面加上时间
 */

//k和v都为String类型
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //1.取出具体数据
        String value = System.currentTimeMillis() + "-" + record.value();
        //2.返回对象
        return new ProducerRecord<String, String>(record.topic(), record.key(), value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
