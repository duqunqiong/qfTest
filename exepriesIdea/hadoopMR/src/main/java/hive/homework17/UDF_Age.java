package hive.homework17;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDF_Age extends UDF {
    //功能函数
    public String  evalute(String string){
        if (string == null) {
            return "";
        }else {
            String[] strings = string.split("-");
            Integer year = Integer.valueOf(strings[0]);
//            Integer month = Integer.valueOf(strings[1]);
            int i =2020-year;
            String s = String.valueOf(i);
            return s;
        }
    }

//    //调试功能函数
//    public static void main(String[] args) {
//        System.out.println(new UDF_Age().evalute("1997-10-29"));
//    }
}
