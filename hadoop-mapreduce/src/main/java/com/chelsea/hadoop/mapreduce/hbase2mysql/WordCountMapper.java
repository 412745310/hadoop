package com.chelsea.hadoop.mapreduce.hbase2mysql;

import java.io.IOException;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单词计算mapper类
 * 
 * @author shevchenko
 *
 */
public class WordCountMapper extends TableMapper<Text, Text> {
    
    private static Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
    private static final String family = "cf";
    private static final String column = "count";

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        String row = Bytes.toString(CellUtil.cloneRow(value.getColumnLatestCell(family.getBytes(), column.getBytes())));
        String count = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family.getBytes(), column.getBytes())));
        logger.debug("mapper-------------> row = " + row + ", count = " + count);
        context.write(new Text(row), new Text(count));
    }

}
