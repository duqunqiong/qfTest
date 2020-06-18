package mapreduce.join.mapjoin;

import mapreduce.DeleteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.net.URI;

public class MapJoinTool extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定jar的位置
        job.setJarByClass(MapJoinTool.class);

        //指定map相关的类
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //没有相关的reduce，map就可以完成
        job.setNumReduceTasks(0);

        //给job中添加一个缓存文件，注意添加的URI格式是file:///e:/...
        job.addCacheFile(new URI("file:///d:/product.txt"));

        //指定输入输出路劲
        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"d:/input11","d:/mapoutput"};
        DeleteUtil.deleteFolder(new File(args[1]));
        ToolRunner.run(new MapJoinTool(), args);

    }
}

