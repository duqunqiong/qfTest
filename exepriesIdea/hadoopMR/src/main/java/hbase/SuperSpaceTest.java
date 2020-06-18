package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public  class SuperSpaceTest {
    protected static Connection connection;
    protected static HBaseAdmin hBaseAdmin;

    /*定义一个对象，初始化连接 Connection 和 HbaseAdmin ，再多个方法之前，只执行一次*/
    @BeforeClass
    public static void beforeClass() throws IOException {
        // 1. 生成配置对象
        Configuration configuration = new Configuration();

        // 2. 配置连接属性
        configuration.set("hbase.zookeeper.quorum", "601-m1:2181,601-s1:2181,601-s2:2181");

        /*// 3. 得到hbase的管理对象 HBaseAdmin
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);*/

        // 3. 工厂模式：生成connection
        connection = ConnectionFactory.createConnection(configuration);

        // 4. 通过连接对象 创建 HBaseAdmin
        Admin admin = connection.getAdmin();
        hBaseAdmin = (HBaseAdmin) admin;

    }

    /* 所有方法执行结束之后， 需要关闭 HBaseAdmin 以及 connection 对象*/
    @AfterClass
    public static void afterClass() {
        // 5.关闭hbaseAdmin 与 connection 连接对象，先开后关闭
        try {
            hBaseAdmin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 通过名字创建一个表
    public static Table getTable(String tablename) throws IOException {

        // table 是用来管理表中的数据，
        Table table = connection.getTable(TableName.valueOf(tablename));
        return table;
    }
}
