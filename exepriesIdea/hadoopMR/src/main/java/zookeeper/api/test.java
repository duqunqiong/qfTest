package zookeeper.api;

//zookeeper的简单测试类：增删改查

import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class test {
    private static ZooKeeper zooKeeper;
    @BeforeClass
    public static void test() throws IOException {
        //新建一个ZK的客户端，传入连接字符串，超时时间，默认的观察者对象
        zooKeeper= new ZooKeeper("Hadoop701Clone2:2181", 20000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("watchedEvent = " + watchedEvent);
            }
        });
        System.out.println("zooKeeper = " + zooKeeper);
    }

    //通过zk客户端创建zk节点
    @Test
    public void testCrateNode() throws KeeperException, InterruptedException {
      zooKeeper.create("/myRoot","myRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //通过客户端得到zk节点的数据
    @Test
    public void testDeleteNode() throws KeeperException, InterruptedException {
        //delete可以删除一个节点，第一个参数是删除的路径，第二个参数是数据版本号，如果是-1表示都删除
        zooKeeper.delete("/myRoot",-1);
        System.out.println("删除成功");
    }

    //因为zookeeper是外边的资源，不在jvm的堆中，所以使用完为了防止内存泄露，一定要再@AfterClass手工关闭
    @AfterClass
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
