package com.chelsea.hadoop.mapreduce.hbase2mysql.collector;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class WordCountCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, Text key, Text value, PreparedStatement pstmt) throws SQLException, IOException {
        // 进行参数设置
        int i = 0;
        pstmt.setString(++i, key.toString());
        pstmt.setInt(++i, Integer.valueOf(value.toString()));

        // 添加到batch中
        pstmt.addBatch();
    }
    
}
