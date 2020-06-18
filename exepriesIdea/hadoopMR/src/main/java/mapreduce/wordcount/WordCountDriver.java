package mapreduce.wordcount;


import mapreduce.DeleteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * 定义一个Driver类,把Mapper和Reducer进行关联,打包成一个任务(Job),并且提交开始执行
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        启动一个Job的流程

        //删除已经存在的output
        DeleteUtil.deleteFolder(new File("args[1]"));

//        1.得到配置文件
        Configuration configuration = new Configuration();
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
//        2.获取一个Job,通过配置文件获取
        Job job = Job.getInstance(configuration);

//        3.指定jar的位置
        job.setJarByClass(WordCountDriver.class);

//        4.指定Mapper运行类
        job.setMapperClass(WordCountMap.class);

//        5.指定Mapper输出的key的类型
        job.setMapOutputKeyClass(Text.class);

//        6.指定Mapper输出的value的类型
        job.setMapOutputValueClass(IntWritable.class);

        //combiner类的实现
        job.setCombinerClass(WordCountCombiner.class);

//        7.指定Reducer运行类
        job.setReducerClass(WordCountReduce.class);

//        8.指定Reducer输出的key的类型
        job.setOutputKeyClass(Text.class);

//        9.指定Reducer输出value的类型
        job.setOutputValueClass(IntWritable.class);

//        10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job,new Path("args[0]"));

//        11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job,new Path("args[1]"));

//        12.提交任务,等待执行完成,如果参数为true,那么打印信息
        boolean b = job.waitForCompletion(true);

//        13.自定义退出,根据job的返回结果0,正常,非零表示有错误
        System.exit(b?0:1);
    }
}
