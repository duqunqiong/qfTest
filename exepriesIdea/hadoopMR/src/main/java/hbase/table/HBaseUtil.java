package hbase.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class HBaseUtil {// 因为connection对象和hBaseAdmin要多个方法或者子类公用,所以把这两个对象从本地变量抽取成字段变量
    protected static Connection connection;
    protected static HBaseAdmin hBaseAdmin;
    private static Logger  logger = LoggerFactory.getLogger(HBaseUtil.class);

    public static HBaseAdmin gethBaseAdmin() {
        return hBaseAdmin;
    }



    static Table table;

    private HBaseUtil() {
    }

    public static HBaseAdmin getHBaseAdmin() throws IOException {
        //3.2 在HBase新的api中,使用连接对象来创建HBaseAdmin,降低了耦合度,提高了扩展性
        hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        return hBaseAdmin;
    }

    /**
     * 类似于懒汉式模式来访问连接对象
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        //连接HBase的API步骤
        // 1.生成配置对象
        if (connection==null) {
            Configuration configuration = new Configuration();

            //2.设置连接属性,连接Hbase除了基本属性外,默认配置的连接属性是zookeeper的连接属性
            configuration.set("hbase.zookeeper.quorum", "h1:2181,h2:2181,h3:2181");

            //3.得到HBase的管理对象HBaseAdmin,通过new关键字获取
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

//        3.1 如果用new创建对象,依赖性比较强,对象细节的处理不方便封装,所以一般用工程方法
//            所以在旧的api中HBaseAdmin是new关键字,新的api是通过工厂模式得到
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    public static void closeAdminAndConnection() {
        //5.关闭连接对象
        try {
            hBaseAdmin.close();
            connection.close();
        } catch (IOException e) {
            logger.error("关闭错误", e);
        }
    }

    public static void closeTable(Table fans) {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("关闭表错误", e);
            }
        }
    }

    public static Table getTable(String string) throws Exception {
//  hBaseAdmin,顾名思义是用来管理表结构,对表数据不能做操作,如果我们要处理表中的数据,那么我们要使用Table

//      Table是用来管理表中的数据,一般用工厂模式获取
        table = connection.getTable(TableName.valueOf(string));
        return table;
    }

    protected static void showResult(Result result) throws IOException {
        //      通过result可以拿到RowKey
        byte[] row = result.getRow();
        logger.info("rowkey:--------------" + new String(row));

        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell current = cellScanner.current();
//            使用clone的方式获取cell中的值,优先推荐clone方式
            String string = "family:" + new String(CellUtil.cloneFamily(current)) + "\t" +
                    "column:" + new String(CellUtil.cloneQualifier(current)) + "\t" +
                    "value:" + new String(CellUtil.cloneValue(current));
            logger.info(string);
        }
    }
    public static void showFilterResult(Filter filter,String tablename) throws Exception {
        // 当前这个方法只有SingleColumnValue有,但Filter没有,只能把父类转化成子类使用
//        在强转的时候一定要加上判断保护,如果是子类或者实现类的化才可以强转
        if (filter instanceof SingleColumnValueFilter) {
            SingleColumnValueFilter singleColumnValueFilter=(SingleColumnValueFilter)filter;
            singleColumnValueFilter.setFilterIfMissing(true);
        }

//        4.构建Scan进行表扫描
        Scan scan = new Scan();

//        5.给Scan设定过滤器
        scan.setFilter(filter);

//        6.获取表对象
        Table table = HBaseUtil.getTable(tablename);

//        7.通过命令模式扫描表,传入Scan
        ResultScanner scanner = table.getScanner(scan);

//        8.遍历迭代器,并打印
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            showResult(result);

        }
}}