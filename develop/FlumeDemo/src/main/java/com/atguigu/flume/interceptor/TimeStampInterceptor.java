package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @ClassName MapReduceDemo-TimeStampInterceptor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月12日21:16 - 周二
 * @Describe com.atguigu.flume.interceptor.TimeStampInterceptor
 */
public class TimeStampInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);
        //获得json中key为ts的value值（时间戳）
        String ts = jsonObject.getString("ts");
        //给timestamp添加字段创建的时间而不是当前的时间，因为会发生零点漂移,会自动识别头信息中的timestamp
        event.getHeaders().put("timestamp", ts);

        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

}
