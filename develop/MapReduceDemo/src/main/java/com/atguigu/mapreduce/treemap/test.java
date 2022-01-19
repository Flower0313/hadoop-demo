package com.atguigu.mapreduce.treemap;

import java.util.Map;
import java.util.TreeMap;

/**
 * @ClassName MapReduceDemo-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月16日20:08 - 周日
 * @Describe
 */
public class test {
    public static void main(String[] args) {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(2, "BB");
        map.put(1, "AA");
        map.put(5, "EE");
        map.put(3, "CC");
        map.put(4, "DD");
        map.put(2, "AA");   //验证重复key是否能够插入
        //使用遍历EntrySet方式

        for (Map.Entry<Integer, String> entry : map.entrySet()) {

            System.out.println("Key:" + entry.getKey() + " --- value:" + entry.getValue());

        }
    }
}
