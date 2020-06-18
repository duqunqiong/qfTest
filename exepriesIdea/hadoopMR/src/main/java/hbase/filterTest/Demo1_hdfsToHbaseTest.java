package hbase.filterTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author: DuQunQinong
 * @date: 2020/3/31 0:00
 */

// 将hdfs中的数据，整合到hbase中去

public class Demo1_hdfsToHbaseTest implements Tool {

    // 1. 创建配置对象
    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "601-m1:2181,601-s1:2181,601-s2:2181";
    private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    private final static String HDFS_CONNECT_VALUE = "hdfs://601-m1:9000";
    private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
    private final static String MAPREDUCE_CONNECT_VALUE = "yarn";

    // 2. 创建Mapper
    public static class HbaseMapper extends Mapper<Writable, Text,Text,Writable>{
        private Text k = new Text();
        private Writable v;


        protected void map(Iterator<Writable> key, Text value, Context context) throws IOException, InterruptedException {
            // 读取一行
            String line = value.toString();

            // 切成字段
            String[] columns = line.split("\t");

            // 切成key、value
            for (String colunmn:columns) {
                String[] kv = colunmn.split(":");
                k.set(kv[0]);
                context.write(k,v);
            }
        }
    }

    // 3. 创建reduce
//    public static class HbaseReducer extends TableReduce<Text,Writable, ImmutableBytesWritable>{
//
//        @Override
//        public void reduce(Text key, Iterator<Writable> values, OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter) throws IOException {
//
//        }
//
//        @Override
//        public void close() throws IOException {
//
//        }
//
//        @Override
//        public void configure(JobConf job) {
//
//        }
//    }


    // 4. 设置参数
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Demo1_hdfsToHbaseTest.class);
        job.setMapperClass(HbaseMapper.class);
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }




}
