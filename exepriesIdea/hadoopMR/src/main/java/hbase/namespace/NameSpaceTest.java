package hbase.namespace;

import hbase.SuperSpaceTest;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import java.io.IOException;

public class NameSpaceTest extends SuperSpaceTest {
    // 1. 新创建一个命名空间 qfuser
    @Test
    public void testCreateNameSpace() throws IOException {
        // 首先创建一个NameSpaceDesciptor 对象， 使用build模式创建
        NamespaceDescriptor qfuser = NamespaceDescriptor.create("qfuser1").build();

        //使用HBaseAdmin 创建NameSpace
        hBaseAdmin.createNamespace(qfuser);
        System.out.println("创建namespace “qfuser” 成功");
    }

    // 2. 列出所有的NameSpace
    @Test
    public void testListNameSpace() throws IOException {
        // 获得namespace 的数组对象
        NamespaceDescriptor[] namespaceDescriptors = hBaseAdmin.listNamespaceDescriptors();

        //遍历打印所有的namespace
        for (NamespaceDescriptor namespaceDescriptor :namespaceDescriptors) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor.getName());
        }
    }

    // 3. 列出NameSpace中的所有表
    @Test
    public void testListNameSpaceTables() throws Exception {
        // 获取名命空间 ns1 中多有表的表名数组
        TableName[] ns1s = hBaseAdmin.listTableNamesByNamespace("ns1");

        // 遍历输出所有表的名字
        for (TableName ns1 :ns1s) {
            System.out.println("ns1 = " + ns1.getNameAsString());
        }
    }


    // 4. 列出其中一张表的所有功能
    @Test
    public void testList() throws IOException {
        HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(TableName.valueOf("ns1:user"));
        System.out.println("tableDescriptor = " + tableDescriptor);
    }

    // 5. 删除NameSpace “qfuser”
    @Test
    public void testDropNameSpace() throws IOException {
        hBaseAdmin.deleteNamespace("qfuser1");
        System.out.println("删除成功");
    }

}
