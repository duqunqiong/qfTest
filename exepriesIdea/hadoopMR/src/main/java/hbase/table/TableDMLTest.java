package hbase.table;

import hbase.SuperSpaceTest;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;


/**
 * @author: DuQunQinong
 * @date: 2020/3/25 21:17
 */

// 定义一个Table 的数据操作测试类

public class TableDMLTest extends SuperSpaceTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Table table;

    {
        try {
            table = getTable("usertable");
        } catch (IOException e) {
            logger.error("获取表失败：" + e);
        }
    }

    @Test
    public void testPut() throws IOException {


        // 使用put对象来封装数据，同理可以用get对象来获取数据
        // * 类似于命令模式：把命令封装成一个对象

        //创建一个list集合，装put对象
        ArrayList<Put> lists = new ArrayList<>();

        // 构建一个Put 对象，并且指定rowkey
        Put put1 = new Put(Bytes.toBytes("001"));
        Put put2 = new Put(Bytes.toBytes("002"));
        Put put3 = new Put(Bytes.toBytes("003"));
        Put put4 = new Put(Bytes.toBytes("004"));
        Put put5 = new Put(Bytes.toBytes("005"));
        Put put6 = new Put(Bytes.toBytes("006"));

        // 对一个Put对象（rowkey），可以插入多列数据  ： 列簇  列名  值
        put1.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("lixi"));
        put2.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("rock"));
        put3.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("lee"));
        put4.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("wyl"));
        put5.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("jy"));
        put6.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("uname"), Bytes.toBytes("hr"));

        put1.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        put2.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        put3.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        put4.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("woman"));
        put5.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("woman"));
        put6.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sex"), Bytes.toBytes("woman"));

        put1.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("1000"));
        put2.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("2000"));
        put3.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("3000"));
        put4.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("4000"));
        put5.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("5000"));
        put6.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("6000"));

        // 将put 数据 添加到list 集合中去
        lists.add(put1);
        lists.add(put2);
        lists.add(put3);
        lists.add(put4);
        lists.add(put5);
        lists.add(put6);

        //批量添加数据
        table.put(lists);

        logger.info("数据批量插入成功");

    }

    // 插入一条数据
    @Test
    public void putOneDataTest() {
        Put put = new Put(Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("sal"), Bytes.toBytes("1000"));
        try {
            table.put(put);
        } catch (IOException e) {
            logger.error("数据插入失败：" + e);
        }
        logger.info("数据插入成功");
    }
}
