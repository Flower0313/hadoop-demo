package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName MapReduceDemo-CounterInterceptor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月31日1:44 - 周五
 * @Describe 统计成功或失败的条数
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int success;
    int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //若不等于null,说明成功了添加了
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功:" + success + ",失败:" + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
