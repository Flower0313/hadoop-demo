package com.atguigu.mapreduce.partitionandcompare;

/**
 * @ClassName MapReduceDemo-FlowBean
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月06日21:57 - 周一
 * @Theme
 */

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.toString()
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow(Long upFlow, Long downFlow) {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    private long sumFlow;

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public FlowBean() {

    }

    /**
     * 序列化,实现序列化和反序列化方法,注意顺序一定要保持一致
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }


    /**
     * 这里的compareTo会在环形缓冲区中进行快速排序
     * 也就是QuickSort中的sortInternal()中调用了此比较方法
     * QuickSort的第74行 => MapTask的第1283行 => WritableComparator的第177行 => 自定义排序方法的compareTo
     * 快速排序的时机就是当缓冲区快要溢写出去的时候
     *
     * @param o 比较对象
     * @return 比较结果
     */
    @Override
    public int compareTo(FlowBean o) {
        //总流量倒序进行排序
        if (this.sumFlow > o.sumFlow) {
            return -1;
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            //相等的话，按照上行流量正序
            if (this.upFlow == o.upFlow) {
                return 0;
            }
            return (this.upFlow > o.sumFlow) ? 1 : -1;
        }
    }
}
