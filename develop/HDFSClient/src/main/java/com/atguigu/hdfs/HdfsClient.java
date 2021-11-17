package com.atguigu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


/**
 * @ClassName HDFSClient-HdfsClient
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月03日23:55 - 周五
 * @Theme
 */

public class HdfsClient {
    private Configuration conf = null;
    private FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("初始化");
        String user = "holdenxiao";
        //拿到NameNode的路径
        URI uri = new URI("hdfs://hadoop102:8020");

        conf = new Configuration();
        //这个配置文件中的参数一就对应了hdfs-site.xml中的name
        //参数二对应了hdfs-site.xml中的value,代表副本数
        conf.set("dfs.replication", "3");

        //获取到客户端对象
        fs = FileSystem.get(uri, conf, user);


        /*或者这样连接
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop102:8020");
        fs = FileSystem.get(conf);*/
    }

    @After
    public void close() throws IOException {
        System.out.println("关闭资源");
        //关闭资源
        fs.close();
    }


    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void testmkdir() throws IOException {
        /*
         * @Before会先执行，然后执行@Test，最后执行@After
         * */
        //创建一个文件
        fs.mkdirs(new Path("/xiyou/shuiliandong"));
    }

    /**
     * 上传
     */
    @Test
    public void testPut() throws IOException {
        //参数一：表示是否删除本地数据
        //参数二：是否允许覆盖
        //参数三：源数据路径
        //参数四：目的地路径
        fs.copyFromLocalFile(false, false,
                new Path("T:\\User\\Desktop\\hadoop-3.1.3.tar.gz"),
                new Path("hdfs://hadoop102///output/hadooptar2"));
    }

    /**
     * 下载
     */
    @Test
    public void testGet() throws IOException {
        //参数一：是否删除原文件
        //参数二：原文件路径
        //参数三：目标地址路径
        //参数四：开启文件校验，开启后会随着文件来一个crc校验文件，一般设置为true不校验，一般是循环冗余校验
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/jinguo/shuguo.txt"),
                new Path("src\\main\\resources\\"), true);
    }


    /**
     * 文件删除
     */
    @Test
    public void testRm() throws IOException {
        //参数一：需要删除的路径
        //参数二：是否递归删除
        //如果删除的是非空文件夹则参数二必须为true，不然报错
        //若是删除空文件夹或文件，则参数二可以为false
        fs.delete(new Path("hdfs://hadoop102/jinguo/weiguo.txt"), false);
    }


    /**
     * 文件移动
     */
    @Test
    public void testMove() throws IOException {
        //参数一：原文件路径
        //参数二：目标文件路径
        //可用于改名或者移动文件
        fs.rename(new Path("hdfs://hadoop102/jinguo/wg.txt"),
                new Path("hdfs://hadoop102/"));
    }

    /**
     * 获取文件详细信息
     */
    @Test
    public void testInfo() throws IOException {
        //参数二：是否递归显示
        RemoteIterator<LocatedFileStatus> lfr = fs.listFiles(
                new Path("hdfs://hadoop102/"), true);
        while (lfr.hasNext()) {
            LocatedFileStatus next = lfr.next();
            //文件路径
            System.out.println("========" + next.getPath() + "========");
            System.out.println("文件权限：" + next.getPermission());
            System.out.println("所有者：" + next.getOwner());
            System.out.println("所在组：" + next.getGroup());
            System.out.println("文件大小：" + next.getLen() + "B");
            System.out.println("上次修改时间：" + next.getModificationTime());
            System.out.println("副本数：" + next.getReplication());
            System.out.println("文件块大小：" + next.getBlockSize() + "B");
            System.out.println("文件名：" + next.getPath().getName());

            // 获取块信息
            BlockLocation[] bL = next.getBlockLocations();
            //bL[0]是文件存的块号，从Block0开始，bL[1]是文件的大小，后面则是存储在哪些服务器上
            System.out.println(Arrays.toString(bL));
        }
    }


    /**
     * 判断是文件夹还是文件
     */
    @Test
    public void testIfFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("hdfs://hadoop102/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("路径：" + status.getPath().getName());
            }
        }


    }
}
