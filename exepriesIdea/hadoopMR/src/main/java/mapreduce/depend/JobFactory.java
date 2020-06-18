package mapreduce.depend;

import mapreduce.DeleteUtil;
import mapreduce.topn.MovieBean;
import mapreduce.topn.MovieDriver;
import mapreduce.topn.MovieMapper;
import mapreduce.topn.MovieReducer;
import mapreduce.wordcount.WordCountDriver;
import mapreduce.wordcount.WordCountMap;
import mapreduce.wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class JobFactory {
    private static String[] args = new String[]{"d:/input2", "d:/output1", "d:/output2", "d:/output3"};

        //构建topn1前10位
    public static Job  TopNFirst () throws IOException {

        Configuration configuration = new Configuration();
        configuration.set("topN","10");
        //设置一个job，并获取配置文件
        Job job = Job.getInstance(configuration,"TopNFirst");

        //设置运行类
        job.setJarByClass(MovieDriver.class);

        //设置map相关类
        job.setMapperClass(MovieMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);

        //设置reduce相关的类
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);

        DeleteUtil.deleteFolder(new File(args[1]));

        //设置输入输出的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job;

    }

    //构建topn2前5位
    public static Job TopNSecond() throws IOException {
        //得到一个配置文件
        Configuration configuration = new Configuration();

        //设置前5
        configuration.set("topN","5");

        //设置一个job，并获取配置文件
        Job job = Job.getInstance(configuration,"TopNSecond");

        //设置运行类
        job.setJarByClass(MovieDriver.class);

        //设置map相关类
        job.setMapperClass(MovieMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);

        //设置reduce相关的类
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);

        DeleteUtil.deleteFolder(new File(args[2]));

        //设置输入输出的路径
        FileInputFormat.setInputPaths(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        return job;
    }

    //构建wordcount的job
    public static Job WordCountJob() throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        DeleteUtil.deleteFolder(new File(args[3]));

        FileInputFormat.setInputPaths(job,new Path(args[2]));
        FileOutputFormat.setOutputPath(job,new Path(args[3]));

        return job;
    }

}
