package com.atguigu.hive.mylen;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * @ClassName MapReduceDemo-MyUDF
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月23日9:26 - 周四
 * @Describe 创建一个计算给定基本数据类型的长度的函数
 * com.atguigu.hive.mylen.MyUDF
 */
public class MyUDF extends GenericUDF {
    /**
     * 1.判断参数的个数
     * 2.判断参数的类型
     * 3.约定返回值类型
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("===请只输入一个参数！===");
        }
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(1, "===只能是primitive类型===");
        }
        //调用工厂来创建类
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 解决具体逻辑的
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if(o==null){
            return 0;
        }
        return o.toString().length();
    }

    /**
     * 用于获取解释的字符串
     * 如果当前函数需要跑mr，会打印一个字符串，内容就是return内容。
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return "该函数是用于获取字符串的长度";
    }
}
