package mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,Order, NullWritable> {
    Order order =new Order();

    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        // value=order01 pro04 225.8
        String s = value.toString();
        String[] s1 =s.split("\t");

        order.setOrderId(s1[0]);
        order.setProId(s1[1]);
        order.setPrice(Double.parseDouble(s1[2]));

        context.write(order,NullWritable.get());

    }
}
