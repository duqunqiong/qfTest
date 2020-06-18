package hive.homework18;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class Key2Value extends UDF {
    //回调方法evalute，用来处理自定义函数的核心业务

    public String evaluate(String str,String key) throws JSONException {
        //判断key与str的值是否为空,直接退出
        if (StringUtils.isEmpty(str) && StringUtils.isEmpty(key)){
            return null;
        }

        //        转化前str:  sex=1&hight=180&weight=130&sal=28000
        //        转化后      {sex:1,hight:180,weight:130,sal:28000}
        //完成转化
        String replace = str.replace("&", ",");
        String replace1 = replace.replace("=", ":");

        //给字符串加上大括号
        String result = "{"+replace1+"}";

        //把字符串转化成json对象
        JSONObject jsonObject = new JSONObject(result);

        //通过Json对象获得其中的key属性
        result = jsonObject.getString(key);
        return result;
    }

//    // 用main方法，不要用单元测试测试简单的转化函数
//    public static void main(String[] args) throws JSONException {
//        new Key2Value().evaluate("sex=1&hight=180&weight=130&sal=28000", "sal");
//    }
}
