package com.atguigu.hbase.bigdata;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.*;
import java.io.IOException;

/**
 * @ClassName MapReduceDemo-TestHbaseAPI_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月05日16:14 - 周二
 * @Describe 过滤器&索引
 */
public class TestHbaseAPI_3 {
    public static void main(String[] args) throws IOException {
        Connection conn = ConnectionFactory.createConnection();
        //2.获取操作对象
        Admin admin = conn.getAdmin();
        String tableName = "student";
        Table table = conn.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //过滤大于等于1002的
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("1002"));
        //用正则筛选出只有三个字符的rowkey
        RegexStringComparator rsc = new RegexStringComparator("^.{3}$");
        Filter f1 = new RowFilter(
                CompareFilter.CompareOp.GREATER_OR_EQUAL,bc
        );
        Filter f2 = new RowFilter(
                CompareFilter.CompareOp.EQUAL,rsc
        );

        //MUST_PASS_ALL:必须通过所有的条件
        //MUST_PASS_ONE:通过其中一个条件即可
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(f1);
        list.addFilter(f2);

        //扫描时增加过滤器
        scan.setFilter(list);

        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("------");
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell))+":"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        conn.close();

    }
}
