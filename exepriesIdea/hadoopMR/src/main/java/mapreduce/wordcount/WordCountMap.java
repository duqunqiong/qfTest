package mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context contex) throws IOException, InterruptedException {

       String string = value.toString();

       String[] strs = string.split(" ");

        for (String word:strs) {
            contex.write(new Text(word),new IntWritable(1));
        }
    }


}
