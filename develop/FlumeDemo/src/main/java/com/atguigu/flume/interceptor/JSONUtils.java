package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;


/**
 * @ClassName MapReduceDemo-JSONUtils
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月12日19:37 - 周二
 * @Describe
 */
public class JSONUtils {
    public static boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        //出现异常返回false
        }catch (JSONException e){
            return false;
        }
    }

}
