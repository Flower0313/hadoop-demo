package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName MapReduceDemo-TypeInterceptor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月28日11:37 - 周二
 * @Describe com.atguigu.flume.TypeInterceptor
 */
public class TypeInterceptor implements Interceptor {
    //声明一个存放事件的集合
    private List<Event> addHeaderEvent;

    @Override
    public void initialize() {
        //初始化存放事件的集合，懒汉加载模式
        addHeaderEvent = new ArrayList<>();
    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //1.获取事件中的头信息,因为Event中的header是以kv形式存储的
        //可以直接event.getHeaders().put("type","atguigu");
        //这里headers能和event产生关系的原因是event.getHeaders()赋值的是一个地址，所以能直接改变
        Map<String,String> headers = event.getHeaders();
        //2.获取事件中的body信息，getBody()封装了将字节数组转换成了string
        String body = new String(event.getBody());
        //type是自定义的名称
        //3.根据body中的是否有"atguigu"来决定添加怎么样的头信息
        if(body.contains("atguigu")){
            //4.分别添加头信息
            headers.put("type","first");
        }else{
            //4.分别添加头信息
            headers.put("type","second");
        }
        return event;
    }

    //批量事件拦截，拦截链
    @Override
    public List<Event> intercept(List<Event> events) {
        //1.清空集合
        addHeaderEvent.clear();

        //2.遍历events
        for (Event event : events) {
            //3.将这些event单独交个上面的单个事件处理
            addHeaderEvent.add(intercept(event));
        }
        //4.返回结果
        return addHeaderEvent;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder{
        //声明内部类Builder,来调用自定义的类
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
