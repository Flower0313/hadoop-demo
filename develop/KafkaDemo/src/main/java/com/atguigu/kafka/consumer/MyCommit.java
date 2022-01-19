package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @ClassName MapReduceDemo-MyCommit
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月19日13:54 - 周三
 * @Describe
 */
public class MyCommit implements OffsetCommitCallback {


    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

    }
}
