package com.chelsea.hadoop.mapreduce.hbase2mysql;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/**
 * 单词计算并写入hbase
 * 
 * @author shevchenko
 *
 */
public class WordCountReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    public static final byte[] CF = "cf".getBytes();
    public static final byte[] COUNT = "count".getBytes();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        
    }

}
