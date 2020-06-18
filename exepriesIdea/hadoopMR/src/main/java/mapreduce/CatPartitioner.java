package mapreduce;

import mapreduce.cattest.Cat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class CatPartitioner extends Partitioner<Cat, NullWritable> {
    //定义一个数据字典，把手机的手机号与对应的分区号联系起来
    private static HashMap<String,Integer> numberMap = new HashMap<>();

    //初始化对应关系
    static {
        numberMap.put("138",0);
        numberMap.put("137",1);
        numberMap.put("136",2);
        numberMap.put("135",3);
    }

    @Override
    public int getPartition(Cat cat, NullWritable nullWritable, int i) {
        String number = cat.getNumber();
        String substring = number.substring(0,3);
        Integer result = numberMap.get(substring);

        if (result == null) {
            result = 3;
        }
        return result;
    }
}
