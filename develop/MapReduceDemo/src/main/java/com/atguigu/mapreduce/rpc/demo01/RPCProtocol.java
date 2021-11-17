package com.atguigu.mapreduce.rpc.demo01;

/**
 * @ClassName MapReduceDemo-RPCProtocol
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月12日13:36 - 周日
 * @Theme 声明自定义的协议，其中hadoop就是这样封装了一系列对文件操作的方法
 */
public interface RPCProtocol {
    long versionID = 313;

    void mkdirs(String path);
}
