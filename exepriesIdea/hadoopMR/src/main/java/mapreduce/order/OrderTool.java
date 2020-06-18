package mapreduce.order;

import mapreduce.DeleteUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class OrderTool extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(OrderTool.class);

        job.setMapperClass(OrderMapper.class);

        job.setMapOutputKeyClass(Order.class);

        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);

        job.setOutputKeyClass(Order.class);

        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        return b?0:1;

    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"d:/input","d:/output"};
        DeleteUtil.deleteFolder(new File(args[1]));
        ToolRunner.run(new OrderTool(),args);
    }
}
