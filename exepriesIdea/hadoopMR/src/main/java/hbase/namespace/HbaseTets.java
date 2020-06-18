package hbase.namespace;

import hbase.SuperSpaceTest;
import org.junit.Test;

import java.io.IOException;

//一个简单的测试类
public class HbaseTets extends SuperSpaceTest {

    @Test
    public void testHBaseEnv() throws IOException {
        System.out.println("hBaseAdmin.tableExists(\"ns1:user\") = " + hBaseAdmin.tableExists("ns1:user"));
    }
}
