package com.chelsea.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
     * 新增序列化对象
     * @throws IOException
     */
    @Test
    public void insertProtobuf() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        String familyName = "cf1";
        String column = "phone";
        Phone.PhoneDetail.Builder phone = Phone.PhoneDetail.newBuilder();
        phone.setDnum("13229345124");
        phone.setLength("10");
        phone.setType("1");
        phone.setDate("20190314");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), phone.build().toByteArray());
        table.put(put);
    }
    
    /**
     * 新增序列化对象集合
     * 
     * @throws IOException
     */
    @Test
    public void insertProtobufList() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        String familyName = "cf1";
        String columnName = "dayPhone";
        Phone.PhoneDetail.Builder phone1 = Phone.PhoneDetail.newBuilder();
        phone1.setDnum("13229345124");
        phone1.setLength("10");
        phone1.setType("1");
        phone1.setDate("20190314");
        Phone.PhoneDetail.Builder phone2 = Phone.PhoneDetail.newBuilder();
        phone2.setDnum("18580251861");
        phone2.setLength("20");
        phone2.setType("0");
        phone2.setDate("20190314");
        Phone.DayPhoneDetail.Builder dayPhone = Phone.DayPhoneDetail.newBuilder();
        dayPhone.addPhoneDetail(phone1);
        dayPhone.addPhoneDetail(phone2);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(familyName.getBytes(), columnName.getBytes(), dayPhone.build().toByteArray());
        Table table = connection.getTable(TableName.valueOf(tableName));
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
     * 查询序列化对象集合
     * 
     * @throws IOException
     */
    @Test
    public void selectProtobufList() throws IOException {
        String tableName = "testTable";
        String rowKey = "1";
        String familyName = "cf1";
        String columnName = "dayPhone";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        Cell cell = result.getColumnLatestCell(familyName.getBytes(), columnName.getBytes());
        Phone.DayPhoneDetail dayPhoneDetail = Phone.DayPhoneDetail.parseFrom(CellUtil.cloneValue(cell));
        for (Phone.PhoneDetail phone : dayPhoneDetail.getPhoneDetailList()) {
            System.out.print(phone.getDnum() + "_" + phone.getLength() + "_" + phone.getType() + "_" + phone.getDate());
            System.out.println();
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
     * 查询所有数据
     * @throws IOException
     */
    @Test
    public void findAll() throws IOException {
        String tableName = "testTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            Cell cellName = res.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            Cell cellAge = res.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
            String name = new String(CellUtil.cloneValue(cellName));
            String age = new String(CellUtil.cloneValue(cellAge));
            System.out.println(name + "_" + age);
        }
    }
    
    /**
     * 按查询条件过滤
     * 
     * @throws IOException
     */
    @Test
    public void findByFilter() throws IOException {
        String tableName = "testTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter filter1 = new PrefixFilter(Bytes.toBytes("2"));
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("zhangsan"));
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("age"), CompareOperator.EQUAL, Bytes.toBytes("20"));
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            Cell cellName = res.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            Cell cellAge = res.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
            String name = new String(CellUtil.cloneValue(cellName));
            String age = new String(CellUtil.cloneValue(cellAge));
            System.out.println(name + "_" +age);
        }
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
