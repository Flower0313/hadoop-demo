import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName MapReduceDemo-Zookeeper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月16日19:33 - 周四
 * @Describe
 */
public class Zookeeper {
    private String connectString;
    private int sessionTimeout;
    private ZooKeeper zkClient;

    @Before //获取客户端对象
    public void init() throws IOException {
        connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        sessionTimeout = 10000;

        //参数一：集群连接字符串
        //参数二：连接超时时间，单位毫秒
        //参数三：当前客户端默认的监控器
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    @After //关闭客户端对象
    public void close() throws InterruptedException {
        zkClient.close();
    }

    //获取子节点列表，不监听
    @Test
    public void ls() throws IOException, KeeperException, InterruptedException {
        //用客户端对象做各种操作
        List<String> children = zkClient.getChildren("/flower", false);
        System.out.println(children);
    }


    //获取子节点并且监听
    @Test
    public void lsAndWatch() throws KeeperException, InterruptedException {
        //只会监听/flower下面的变化，不会监控/flower子目录下面的变化
        //整个lsAndWatch()方法是在main线程下的，但是process是在服务端线程中开启的。
        //Q:不加sleep为啥接收不到监听内容？
        //A:不加sleep的话客户端关闭，那么服务端也将客户端注册的监听内容退出了，所以服务端啥也没监听到。
        List<String> children = zkClient.getChildren("/flower", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                System.out.println("监视器检查到了变化");
                System.out.println("监视线程:" + Thread.currentThread().getName());
                //监控到了情况直接汇报后结束
                System.exit(0);
            }
        });
        System.out.println(children);
        //不加线程睡眠会直接结束，加了线程等待，就会一直等待直到发生监视
        System.out.println("主线程:" + Thread.currentThread().getName());
        Thread.currentThread().sleep(Long.MAX_VALUE);
    }

    //创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        //参数解读 1节点路径  2节点存储的数据
        //3节点的权限(使用Ids选个OPEN即可) 4节点类型 短暂 持久 短暂带序号 持久带序号
        String path = zkClient.create("/atguigu", "shanguigu".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //创建临时节点
        /*String path = zkClient.create("/flower", "shanguigu".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);*/
        System.out.println(path);

        //创建临时节点的话,需要线程阻塞
        //Thread.sleep(10000);
    }

    //判断子节点是否存在
    @Test
    public void exist() throws Exception {
        Stat stat = zkClient.exists("/flower", false);
        System.out.println(stat == null ? "不存在" : "存在");
    }

    //获取子节点的数据，不监听
    @Test
    public void get() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/flower", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }

        byte[] data = zkClient.getData("/flower", false, stat);
        System.out.println(new String(data));
    }

    //获取子节点存储的数据，并监听
    @Test
    public void getAndWatch() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/flower/holdenxiao", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }
        byte[] data = zkClient.getData("/flower", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        }, stat);
        if (data != null) {
            System.out.println(data.toString());
        }
        //线程阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    //设置节点的值
    @Test
    public void set() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/flower", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }
        //参数解读 1节点路径 2节点的值 3版本号
        zkClient.setData("/flower", "sgg".getBytes(), stat.getVersion());
    }

    //删除空节点
    @Test
    public void delete() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/flower2222", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }
        //参数二是版本号，因为每改一次，文件都会自增一次，所以要么自动获取要么直接填-1
        zkClient.delete("/flower", stat.getVersion());
    }

    //封装一个方法,方便递归调用
    //测试deleteAll
    @Test
    public void testDeleteAll() throws KeeperException, InterruptedException {
        deleteAll("/flower", zkClient);
    }

    //判断路径下的节点是否存在
    public boolean isExist(String path) throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists(path,false);
        if(stat == null)
            return false;
        return true;
    }


    public void deleteAll(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }
        //先获取当前传入节点下的所有子节点
        List<String> children = zk.getChildren(path, false);
        if (children.isEmpty()) {
            //说明传入的节点没有子节点,可以直接删除
            zk.delete(path, stat.getVersion());
        } else {
            //如果传入的节点有子节点,循环所有子节点
            for (String child : children) {
                //有节点直接传递进去拼接
                //删除子节点,但是不知道子节点下面还有没有子节点,所以递归调用
                deleteAll(path + "/" + child, zk);
            }
            //删除完所有子节点以后,记得删除传入的节点
            zk.delete(path, stat.getVersion());
        }
    }

    //获取子节点的列表，并循环监听(监听节点的路径而不是内容)
    public void lsAndWatch(String path) throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                System.out.println("flower的路径发生了变化");
                //只有上一个监听到了数据才会继续走开启一个监听，而不是一直递归监听，所以不会溢出
                try {
                    lsAndWatch(path);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println(children);
    }

    //开启循环监听
    @Test
    public void testWatch() throws KeeperException, InterruptedException {
        lsAndWatch("/flower");
        Thread.sleep(Long.MAX_VALUE);
    }


}
