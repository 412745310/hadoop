package com.chelsea.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
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
    private Connection connection = null;
    
    /**
     * hbase客户端连接初始化
     * @throws IOException 
     */
    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "47.107.247.223");
        connection = ConnectionFactory.createConnection(conf);
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
            familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName[i])).build());
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
     * 新增数据
     * @throws IOException 
     */
    @Test
    public void insert() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        String familyName = "cf1";
        String column1 = "name";
        String value1 = "lisi";
        String column2 = "age";
        String value2 = "25";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1), Bytes.toBytes(value1));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column2), Bytes.toBytes(value2));
        table.put(put);
    }
    
    /**
     * 查询数据
     * @throws IOException 
     */
    @Test
    public void select() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        String familyName = "cf1";
        String column1 = "name";
        String column2 = "age";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column2));
        Result result = table.get(get);
        while (result.advance()) {
            Cell cell = result.current();
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println(family + ":" + column + ":" + value);
        }
    }
    
    /**
     * 删除数据
     * 
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        deleteList.add(delete);
//        table.delete(delete);
        table.delete(deleteList);
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
