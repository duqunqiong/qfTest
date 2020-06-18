package mapreduce.topn;

import mapreduce.DeleteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class MovieDriver {

   /* public static void Runner(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
       *//*conf.set("topN","10");*//*

        Job job = Job.getInstance(conf,"rate");
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        //System.out.println("topN = " + conf.get("topN"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);

        *//*FileInputFormat.addInputPath(job,new Path(args[0]));*//*
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        int i = job.waitForCompletion(true) ? 0 : 1;

//        //得到所有计数器；
//        Counters jobCounters = job.getCounters();
//
//        //用程序拿到具体的计数器的值，并进行相应的处理----枚举创建的计数器
//        Counter counter = jobCounters.findCounter(Level2.INFO);
//        long numberOfRun = counter.getValue();
//        System.err.println("numberOfRun = " + numberOfRun);
//
//        //拿到具体的运行次数的值------自定义创建的计数器
//        Counter counter1 = jobCounters.findCounter("user", "user1");
//        long numberOfRun2 = counter1.getValue();
//        System.out.println("numberOfRun2 = " + numberOfRun2);
//
//        //获取系统内所有计数器的名字，并获取计数器的值
//        for (CounterGroup counterGroup:jobCounters) {
//            System.err.println("counterGroupName = " + counterGroup.getDisplayName()+"---------");
//            for (Counter counter2:counterGroup) {
//                System.err.println("counter2.getName() = " + counter2.getName());
//                System.err.println("counter2.getValue() = " + counter2.getValue());
//            }
//        }

        System.exit(i);

    }
*/
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[]{"d:/input2","d:/output"};
        DeleteUtil.deleteFolder(new File(args[1]));
        Configuration conf = new Configuration();
        conf.set("topN","10");

        Job job = Job.getInstance(conf,"rate");
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        //System.out.println("topN = " + conf.get("topN"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);

        /*FileInputFormat.addInputPath(job,new Path(args[0]));*/
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        int i = job.waitForCompletion(true) ? 0 : 1;
        System.exit(i);
    }
}
