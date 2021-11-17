package com.atguigu.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TableMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月09日11:17 - 周四
 * @Theme
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    //这里有两个文件，那么肯定是有两个maptask并行，所以他们业务逻辑一样，也不影响
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        //初始化order.txt 和 pd.txt ，获取他们的名称
        FileSplit split = (FileSplit) context.getInputSplit();
        //一个文件一个切片,一个文件开启一个MapTask所以获取一次就好
        //这里获取的filename是order.txt或pd.txt
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //判断哪张表的哪个文件
        if (fileName.contains("order")) {//处理的订单表
            //这里进来的格式为：1001 01 1
            String[] split = line.split("\\s+");
            //封装kv
            outK.set(split[1]);

            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {//处理的商品表
            //这里进来的格式为: 01 小米
            String[] split = line.split("\\s+");
            outK.set(split[0]);

            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        context.write(outK, outV);
    }
}
