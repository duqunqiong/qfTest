package com.qf.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException, URISyntaxException, InterruptedException {
        System.out.println( "Hello World!" );
        testCopyFromlocal();
    }

    public static void testCopyFromlocal() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://Hadoop701Clone2:9000"),configuration,"atguigu");

        //上传文件
        fileSystem.copyFromLocalFile(new Path("d:/banzhang.txt"),new Path("/banzhang.txt"));

        //关闭资源
        fileSystem.close();
        System.out.println("over");
    }
}
