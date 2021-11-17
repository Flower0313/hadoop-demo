package com.atguigu.mapreduce.utils;

import java.io.*;
import java.util.Properties;

/**
 * @ClassName MapReduceDemo-FileUtils
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月10日10:40 - 周五
 * @Theme
 */
public class FileUtils {
    private final static String PROPATH = "src\\main\\resources\\filenum.properties";
    private final static String PATH = "T:\\ShangGuiGu\\hadoop\\output\\output";
    /**
     * 方式一：通过File文件和for循环遍历所有文件名来创建不同的文件
     *
     * @param cname 类名
     * @return
     */
    public static String getNewFileName(String cname) {
        //获取当前目录下的文件以及文件夹的名称。
        File dir = new File(PATH);
        String[] names = dir.list();
        int lastNum = 1;
        for (String name : names) {
            int temp = 0;
            String[] ts = name.split("put");
            if ((temp = Integer.parseInt(ts[1])) > lastNum) {
                lastNum = temp;
            }
        }
        //返回最新不重复的文件名output8
        return PATH + (lastNum + 1) + "_" + cname;
    }

    /**
     * 方式二：通过读取Properties配置文件获取不同的文件名
     *
     * @param cname 类名
     * @return
     */
    public static String getProFileName(String cname) {
        Properties info = new Properties();
        int nowFileName = 0;
        try {
            info.load(new FileInputStream(PROPATH));
            nowFileName = Integer.parseInt(info.getProperty("lastNum"));
            info.setProperty("lastNum", String.valueOf(nowFileName + 1));
            info.store(new FileOutputStream(PROPATH), null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return PATH + nowFileName + "_" + cname;
    }
}
