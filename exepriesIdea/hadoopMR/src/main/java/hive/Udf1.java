package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Udf1 extends UDF {

    public String toUpper(String param) {
       if (param == null){
           return "";
       }else {
           return param.toUpperCase();
       }
    }
}
