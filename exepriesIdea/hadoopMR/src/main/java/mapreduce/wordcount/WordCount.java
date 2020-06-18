package mapreduce.wordcount;

import mapreduce.DeleteUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;


public class WordCount extends Configured implements Tool {
   /* public String[] args;*/

    @Override
    public int run(String[] args) throws Exception {
        //启动流程


        //2.获取一个Job,通过配置文件获取,把原来的自己new的Conf改变为从父类中传入Conf,用了模版模式
        Job job = Job.getInstance(getConf());

        //指定jar的位置
        job.setJarByClass(WordCount.class);

        //指定Mapper运行类
        job.setMapperClass(WordCountMap.class);

        //指定Mapper输出的key的类型
        job.setMapOutputKeyClass(Text.class);

        //指定Mapper输出的value的类型
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountCombiner.class);


        //指定Reduce运行类
        job.setReducerClass(WordCountReduce.class);

        //指定Reduce输出的key的类型
        job.setMapOutputKeyClass(Text.class);

        //指定Reduce输出的value的类型
        job.setOutputValueClass(IntWritable.class);

        //指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        //通过FileOutputFormat设置输出文件的压缩,并且可以设置具体提的压缩编码
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);



        //提交任务，等待执行完成，如果参数为true，那么打印信息
        boolean b = job.waitForCompletion(true);

        //自定义退出，根据job的返回结果0，正常，非0，表示有错误
        return (b?0:1);

    }

    //在main方法中传入ToolRunner来启动Job，传入的是tool的实现类
    public static void main(String[] args) throws Exception {
        args = new String[]{"d:/input.bz2","d:/output.bz2"};
        DeleteUtil.deleteFolder(new File(args[1]));
        ToolRunner.run(new WordCount(),args);
    }


}
