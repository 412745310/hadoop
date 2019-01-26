package com.chelsea.hadoop.hdfs;

import java.io.FileInputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS测试类
 * 
 * @author shevchenko
 *
 */
public class HDFSTest {
  
    private FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://47.107.247.223:9000"), conf, "dev");
    }

    @After
    public void destroy() throws Exception {
        fs.close();
    }

    /**
     * 创建文件
     * 
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        fs.create(new Path("/helloByJava"));
    }

    /**
     * 下载文件
     * 
     * @throws Exception
     */
    @Test
    public void download() throws Exception {
        fs.copyToLocalFile(new Path("/helloByJava"), new Path("d://"));
    }
    
    /**
     * 文件列表
     * 
     * @throws Exception
     */
    @Test
    public void listFile() throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocationArr = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocationArr) {
                System.out.println("块大小：" + bl.getLength() + " -- 块偏移量：" + bl.getOffset());
                String[] hostArr = bl.getHosts();
                for (String host : hostArr) {
                    System.out.println(host);
                }
            }
            System.out.println("\n");
        }
    }
    
    /**
     * 上传文件
     * 
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {
        fs.copyFromLocalFile(new Path("d:/test1.txt"), new Path("/"));
    }
    
    /**
     * 流的形式上传文件
     * 
     * @throws Exception
     */
    @Test
    public void uploadByStream() throws Exception {
        FSDataOutputStream outputStream = fs.create(new Path("/test2.txt"), true);
        FileInputStream inputStream = new FileInputStream("d:/test2.txt");
        IOUtils.copy(inputStream, outputStream);
    }
    
    /**
     * 删除文件
     * 
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        fs.delete(new Path("/test1.txt"), true);
    }

}
