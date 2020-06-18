package mapreduce.topn;

import mapreduce.Counter.Level2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable, Text,Text,MovieBean> {

    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        MovieBean movieBean = new MovieBean();
        String string = value.toString();
        String[] strings = string.split("\t");
        movieBean.setMovie(strings[0]);
        movieBean.setRate(strings[1]);
        movieBean.setTimeStamp(strings[2]);
        movieBean.setUserId(strings[3]);

//        //通过枚举类定义一个计数器。
//        Counter counter = context.getCounter(Level2.INFO);
//
//        //计数器递增
//        counter.increment(1);
//
//        //通过组名构建一个计数器
//        Counter counter1 = context.getCounter("count", "user1");
//
//        //计数器自增,记录mapper循环执行了多少次
//        if ("user1".equals(strings[3])){
//            counter1.increment(1);
//        }
//
//        //打印评分大于10的电影数目
//        int rateNumber = Integer.parseInt(strings[1]);
//        Counter good = context.getCounter("rate","good");
//        //打印user1评价出的好电影的数量
//        Counter usernum = context.getCounter("user","user1");
//        if (rateNumber > 10){
//            good.increment(1);
//            if (strings[3].equals("user1")) {
//                usernum.increment(1);
//            }
//        }


        context.write(new Text(strings[3]),movieBean);
    }


}
