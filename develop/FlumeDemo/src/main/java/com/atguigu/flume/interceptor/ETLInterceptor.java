package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.atguigu.flume.TypeInterceptor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;


/**
 * @ClassName MapReduceDemo-ETLInterceptor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月12日19:38 - 周二
 * @Describe com.atguigu.flume.interceptor.ETLInterceptor
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        //获取到event中的body值
        String log = new String(body, StandardCharsets.UTF_8);
        //若是标准的日志格式就放入
        if (JSONUtils.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()){
            Event next = iterator.next();
            if(intercept(next)==null){
                iterator.remove();
            }
        }
        return events;
    }



    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override
        public void configure(Context context) {
        }
    }

    @Override
    public void close() {

    }
}
