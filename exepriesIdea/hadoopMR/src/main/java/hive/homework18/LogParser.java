package hive.homework18;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//定义自定义函数使用正则表达式
public class LogParser extends UDF {
    /*
    解析前:220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] "GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1" 200 8784 "-" "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
    解析后:220.181.108.151	20120131 120232	GET	/home.php?mod=space&uid=158&do=album&view=me&from=space	HTTP	200	Mozilla
     */
    public String evaluate(String log) throws ParseException {
        //定义一个正则表达式，对原始的字符串解析，并进行匹配分组
        String reg ="^([0-9.]+\\d+) - - \\[(.*\\+\\d+)\\] .+(GET|POST) (.+) (HTTP)\\S+ (\\d+) .+\\\"(\\w+).+$";

        //通过编译的方式得到匹配器去匹配传入的字符串
        Pattern pattern = Pattern.compile(reg);

        //用模式匹配器去匹配传入的字符串
        Matcher matcher = pattern.matcher(log);

        //定义一个字符串StringBuffer用来拼接字符串
        StringBuffer stringBuffer = new StringBuffer();

        //判断输入的数据是否匹配分组正则表达式的模式
        if (matcher.find()){
            //先获取匹配的分组数
            int groupCount = matcher.groupCount();

            //第一组是下标从1开始
            for (int j =1 ; j<= groupCount;j++){
                //对正则表达式中进行分组遍历，如果是第二组，那么就进行日期格式得转换
                if(j == 2) {
                    //定义一个英文格式得日期格式解析器 ， 原格式为:31/Jan/2012:00:02:32 +0800
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
                    //拿出第二组得数据，通过上述格式化类转化为标准得日期格式
                    Date parseDate = simpleDateFormat.parse(matcher.group(2));

                    //定义转化后的格式  20120131 120232
                    SimpleDateFormat yyyyMMdd_hhmmss = new SimpleDateFormat("yyyyMMdd hhmmss");

                    //把日期按照自定义的格式转化
                    String format = yyyyMMdd_hhmmss.format(parseDate);
                    stringBuffer.append(format+"\t");
                }else {
                    //否则直接拼接
                    stringBuffer.append(matcher.group(j) + "\t");
                }
            }
            return stringBuffer.toString();
        }
        return "";
    }

//    public static void main(String[] args) throws ParseException {
//        String evaluate = new LogParser().evaluate("220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\" \"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"");
//        System.out.println(evaluate);
//    }
}
