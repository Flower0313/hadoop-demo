package com.atguigu.mapreduce.writable;

/**
 * @ClassName MapReduceDemo-FlowBean
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月06日21:57 - 周一
 * @Theme
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.toString()
 */
public class FlowBean implements Writable {
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

    /**
     * 累加求和
     *
     * @param upFlow   上行流量
     * @param downFlow 下载流量
     */
    public void setSumFlow(long upFlow, long downFlow) {
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
     * 因为read是根据上面传输的顺序来读的，所以顺序一定要保持一致
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
}
