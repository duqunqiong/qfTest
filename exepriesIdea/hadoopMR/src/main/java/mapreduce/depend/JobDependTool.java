package mapreduce.depend;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

public class JobDependTool {
    public static void main(String[] args) throws IOException, InterruptedException {

        //先通过工厂模式生成几个job
        Job topNFirst = JobFactory.TopNFirst();
        Job topNSecond = JobFactory.TopNSecond();
        Job wordCountJob = JobFactory.WordCountJob();

        //定义controlledjob,来设定job之间的关系，并且构造器传入要包装的job的conf
        ControlledJob controlledTopNFirstJob = new ControlledJob(topNFirst.getConfiguration());
        ControlledJob controlledTopNSecondJob = new ControlledJob(topNSecond.getConfiguration());
        ControlledJob controlledWordCountJob = new ControlledJob(wordCountJob.getConfiguration());

        // 对controlledjob的原生job进行设置
        controlledTopNFirstJob.setJob(topNFirst);
        controlledTopNSecondJob.setJob(topNSecond);
        controlledWordCountJob.setJob(wordCountJob);

        //设置三者之间的关系
        controlledTopNSecondJob.addDependingJob(controlledTopNFirstJob);
        controlledWordCountJob.addDependingJob(controlledTopNSecondJob);

        //定义jobcontrol，把新建的ControlledJob加入，类似于组的概念
        JobControl jobControl = new JobControl("QFJobControl");
        jobControl.addJob(controlledWordCountJob);
        jobControl.addJob(controlledTopNFirstJob);
        jobControl.addJob(controlledTopNSecondJob);

        //定义jobControl传入到多线程的构建函数中，用多线程的方式启动jobcontrol
        Thread thread = new Thread(jobControl);

        //启动多线程
        thread.start();

        //等到所有的线程都启动完成后，再停止jobcontrol
        while (!jobControl.allFinished()){
            thread.sleep(800);
        }

        //关闭当前的JobControl
        jobControl.stop();
    }
}
