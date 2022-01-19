package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName MapReduceDemo-MySink
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月18日19:59 - 周二
 * @Describe
 */
public class MySink extends AbstractSink implements Configurable {
    //创建Logger对象
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        //在配置表那边配置的名字，比如a1.sinks.k1.flowerName
        prefix = context.getString("flowerName");

    }

    @Override
    public Status process() throws EventDeliveryException {
        //1.获取Channel并开启事务
        Channel channel = getChannel();

        Transaction transaction = channel.getTransaction();
        //2.开启take事务
        transaction.begin();

        try {
            Event event;
            //3.循环拉取channel中的event,直到拉不到为止
            while (true) {
                event = channel.take();
                if (event != null) {
                    //拉取到event了就跳出去
                    break;
                }
            }
            //4.处理事件

            LOG.info("hello,flower!" + new String(event.getBody()));
            //4.提交事务
            transaction.commit();
        } catch (Exception e) {
            //出错回滚
            transaction.rollback();
        } finally {
            //关闭事务
            transaction.close();
        }

        return null;
    }
}
