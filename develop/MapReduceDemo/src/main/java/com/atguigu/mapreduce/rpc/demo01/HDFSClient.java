package com.atguigu.mapreduce.rpc.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @ClassName MapReduceDemo-HDFSClient
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月12日13:43 - 周日
 * @Theme 客户端
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException {
        //客户端对象
        RPCProtocol client = RPC.getProxy(RPCProtocol.class, RPCProtocol.versionID,
                new InetSocketAddress("localhost", 8888), new Configuration());

        System.out.println("客户端开始工作");
        client.mkdirs("/input");

        /*InetAddress address = InetAddress.getByName("192.168.1.11");
        System.out.println(address.getHostName());//Holdenxiao
        System.out.println(address.getHostAddress());*/
    }
}
