package com.atguigu.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @ClassName MapReduceDemo-TableReducer
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日11:56 - 周四
 * @Theme
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        /*
         * 传入进来的数据：
         * 01 1001 " " 1 order
         * 01 1004 " " 4 order
         * 01  0  小米 0 pd
         * */
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBeans = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                TableBean tempOrderBean = new TableBean();
                //这样添加的目的是，若直接add(value)，因为hadoop组件中的迭代器每次迭代的内容不同，但是会用同一个地址，小顶堆
                //所以直接add是一样的地址，所以后面的内容会覆盖之前的内容。
                try {
                    BeanUtils.copyProperties(tempOrderBean, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(tempOrderBean);
            } else {
                //因为进来的数据都是根据id相同的进行循环，而一个id对应一个名称，这里我们只需要name，所以覆盖就覆盖吧。
                try {
                    BeanUtils.copyProperties(pdBeans, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBeans.getPname());
            context.write(orderBean, NullWritable.get());
        }
    }
}
