package com.chelsea.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * hbase java API测试
 * 
 * @author shevchenko
 *
 */
public class HBaseTest {
    
    private Admin admin = null;
    
    /**
     * hbase客户端连接初始化
     * @throws IOException 
     */
    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "47.107.247.223");
        Connection connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }
    
    /**
     * 创建表
     * 
     * @throws IOException 
     */
    @Test
    public void createTable() throws IOException {
        String tableName = "testTable";
        String[] familyName = {"cf1", "cf2"};
        
        List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(familyName.length);
        for (int i = 0; i < familyName.length; i++) {
            familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(familyName[i].getBytes()).build());
        }
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamilies(familyDescriptors)
                .build();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return;
        }
        admin.createTable(tableDescriptor);
    }
    
    /**
     * 释放连接
     * @throws IOException 
     */
    @Test
    public void destroy() throws IOException {
        if (admin != null) {
            admin.close();
        }
    }
    
}
