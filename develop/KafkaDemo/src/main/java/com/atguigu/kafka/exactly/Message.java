package com.atguigu.kafka.exactly;

import java.util.Date;

/**
 * @ClassName MapReduceDemo-Message
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月19日16:08 - 周三
 * @Describe
 */
public class Message {
    private String id;//groupId
    private String name;//topicName
    private String desc;//message
    private Date time = null;

    public Message(String id, String name, String desc, Date time) {
        this.id = id;
        this.name = name;
        this.desc = desc;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", time=" + time +
                '}';
    }
}
