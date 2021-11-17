package com.atguigu.mapreduce.mapjoincompara;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TableBean
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日11:12 - 周四
 * @Theme
 */
public class TableBean2 implements Writable {
    private String id; //订单id
    private String pid; //产品id
    private int amount; //产品数量
    private String pname; //产品名称
    private String flag; //判断是order表还是pd表的标志字段

    //反射需要使用
    public TableBean2() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        // 写入文件的格式 id pname amount
        return id + "\t" + pname + "\t" + amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //String类型用UTF写
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();
    }


}
