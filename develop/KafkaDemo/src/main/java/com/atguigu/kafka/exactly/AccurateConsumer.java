package com.atguigu.kafka.exactly;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

/**
 * @ClassName MapReduceDemo-AccurateConsumer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月19日15:33 - 周三
 * @Describe 利用mysql维护kafka偏移量
 */
public class AccurateConsumer {
    private static final Properties props = new Properties();
    private static final String GROUP_ID = "flower";

    static {
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    //申明消费者
    final KafkaConsumer<String, String> consumer;

    //记录每次消费时每个partition的最新offset
    private Map<TopicPartition, Long> partitionOffsetMap;

    //用于缓存接收消息，然后进行批量入库
    private List<Message> list;

    private final String topicName;
    private final String topicNameAndGroupId;

    private volatile boolean isRunning = true;


    public AccurateConsumer(String topicName) {
        this.topicName = topicName;
        topicNameAndGroupId = topicName + "_" + GROUP_ID;
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        list = new ArrayList<>(100);
        partitionOffsetMap = new HashMap<>();
    }

    /*
     * 目的:实现读取事务性，读取和写入是一次事务
     *
     * */
    public void receiveMsg() throws SQLException {
        Connection conn = null;
        Set<TopicPartition> assignment = new HashSet<>();
        //具体解释:https://weread.qq.com/web/reader/e9a32a0071848698e9a39b8k70e32fb021170efdf2eca12
        while (assignment.size() == 0) {//若不为0，则证明分配到了分区
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        //设置偏移量
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, getOffset("test"));
        }

        try {
            while (isRunning) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(3));
                if (!consumerRecords.isEmpty()) {
                    conn = MysqlUtil.init();
                    conn.setAutoCommit(false);
                    for (TopicPartition topicPartition : consumerRecords.partitions()) {
                        System.out.println(topicPartition.topic() + topicPartition.partition());
                        /*
                         * Explain
                         *  topicPartition是topic-0
                         *  records.size()是1秒中有几条数据,相当于1秒的批次中有几条数据
                         *  records.get(records.size() - 1).offset()是当前分区最后一条偏移量
                         * */
                        List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                        for (ConsumerRecord<String, String> record : records) {
                            //使用fastjson将记录中的值转换为Message对象,并添加到list中
                            list.add(new Message(record.topic(), GROUP_ID, record.value(), null));
                        }
                        System.out.println(records.get(records.size() - 1).offset() + 1);
                        //将partition对应的offset信息添加到map中,入库时将offset-partition信息一起进行入库
                        //先获得这批数据有多少，再获取这批数据最后的偏移量
                        partitionOffsetMap.put(topicPartition, records.get(records.size() - 1)
                                .offset() + 1);//记住这里一定要加1,因为下次消费的位置就是从+1的位置开始
                    }
                }
                if (list.size() > 0) {
                    boolean isSuccess = insertIntoDB(conn, partitionOffsetMap);
                    if (isSuccess) {
                        list.clear();
                        partitionOffsetMap.clear();
                    }
                }
                conn.commit();
            }
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            System.out.println(e.getMessage());
        } finally {
            MysqlUtil.close();
            System.out.println("finally");
        }
    }


    public boolean insertIntoDB(Connection conn, Map<TopicPartition, Long> partitionOffsetMap) throws SQLException {

        boolean flag = false;
        String sql = "";
        PreparedStatement ps = null;
        try {
            for (Map.Entry<TopicPartition, Long> entry : partitionOffsetMap.entrySet()) {
                TopicPartition key = entry.getKey();
                Long offset = entry.getValue();
                String topic_group = key.topic() + "_" + GROUP_ID;

                //这样写相当于upsert
                sql = "insert into kafka_info values('" + topic_group + "_" + key
                        .partition() + "','" + topic_group + "','" + key.partition() + "'," + offset + ")" +
                        " ON DUPLICATE KEY" +
                        " UPDATE offsets = '" + offset + "'";
            }
            ps = conn.prepareStatement(sql);
            ps.execute();
            flag = true;
        } catch (Exception e) {
            assert ps != null;
            ps.close();
            System.out.println(e.getMessage());
        }
        return flag;
    }

    //从mysql中获取偏移量
    public int getOffset(String topicName) throws SQLException {
        Connection conn = MysqlUtil.init();

        PreparedStatement ps = null;
        Integer res = 0;
        String sql = "select offsets from kafka_info where topic_group_partition='test_flower_0'";

        try {
            ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                res = Integer.valueOf(resultSet.getString("offsets"));
                System.out.println(">>>>" + res);
            }

            return res;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        return res;
    }
}
