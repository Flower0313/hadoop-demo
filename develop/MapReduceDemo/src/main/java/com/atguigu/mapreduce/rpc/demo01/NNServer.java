package com.atguigu.mapreduce.rpc.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-NNServer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月12日13:37 - 周日
 * @Theme 服务端，一直会打开等待客户端的操作
 */
//实现通信接口
public class NNServer implements RPCProtocol{
    public static void main(String[] args) throws IOException {
        //启动服务
        //Q：为什么能set这么多次呢？
        //A：因为每个set方法返回的都是Builder对象（源码中是return this），所以对象又可以set，依次循环。
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhsot")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();
        System.out.println("服务器开始工作~！");
        server.start();
    }

    @Override
    public void mkdirs(String path) {
        System.out.println("服务器接收到请求："+path);

    }
}
