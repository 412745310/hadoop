package com.chelsea.hadoop.mapreduce.hbase2mysql.collector;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * 自定义的配合自定义output进行具体sql输出的类
 * 
 * @author root
 *
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * 
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @throws SQLException
     * @throws IOException
     */
    public void collect(Configuration conf, Text key, Text value, PreparedStatement pstmt) throws SQLException, IOException;
}
