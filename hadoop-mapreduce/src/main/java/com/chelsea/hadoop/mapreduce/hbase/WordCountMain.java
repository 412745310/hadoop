package com.chelsea.hadoop.mapreduce.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 单词计算运行主类
 * 
 * @author shevchenko
 *
 */
public class WordCountMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://47.107.247.223:9000");
        conf.set("hbase.zookeeper.quorum", "47.107.247.223");
        Job job = Job.getInstance(conf);
        // 指定jar包运行主类
        job.setJarByClass(WordCountMain.class);
        // 指定mapper类
        job.setMapperClass(WordCountMapper.class);
        // 指定mapper类输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 指定mapper类输出value类型
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob("wordCount", WordCountReducer.class, job, null, null, null, null, false);
        // 指定mr数据的输入路径
        FileInputFormat.setInputPaths(job, "/wordcount/input");
        // 提交程序，并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
