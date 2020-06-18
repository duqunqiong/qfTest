package mapreduce.join.mapjoin;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text,Text> {
    private Map<String,String> productMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();

        String path = cacheFiles[0].getPath();

        Reader reader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = "";

        while ((line=bufferedReader.readLine())!= null){
            String[] split = line.split("\t");
            productMap.put(split[0],split[1]);
        }

        bufferedReader.close();
        reader.close();
        IOUtils.closeStream(bufferedReader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        String pid = split[2];
        int amount = Integer.parseInt(split[3]);
        String pName = productMap.get(pid);
        context.write(new Text(pid),new Text(pName+"\t"+amount));

    }
}
