package hbase.hbaseCorprocesser;

import hbase.SuperSpaceTest;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: DuQunQinong
 * @date: 2020/3/31 21:43
 */
public class Demo2 extends SuperSpaceTest {
    public static void main(String[] args) throws IOException {
        // 定义一个二维数组的切分点
        byte[][] splitKeys = {
                Bytes.toBytes("1000"), Bytes.toBytes("2000"),
                Bytes.toBytes("3000"), Bytes.toBytes("4000")
        };

        // 创建表的描述器
        HTableDescriptor js1 = new HTableDescriptor(TableName.valueOf("js1"));
        HColumnDescriptor info = new HColumnDescriptor("info");

        // 向表中添加列簇
        js1.addFamily(info);

        // 提交创建：方法一
        hBaseAdmin.createTable(js1,splitKeys);

        // 提交创建：方法二
        //hBaseAdmin.createTable(js1,Bytes.toBytes("0"),Bytes.toBytes("6000"),5);


    }
}
