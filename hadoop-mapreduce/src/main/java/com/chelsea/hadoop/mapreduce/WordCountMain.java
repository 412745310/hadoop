package com.chelsea.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 单词计算运行主类
 * 
 * @author shevchenko
 *
 */
public class WordCountMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 指定jar包运行主类
        job.setJarByClass(WordCountMain.class);
        // 指定mapper类
        job.setMapperClass(WordCountMapper.class);
        // 指定reducer类
        job.setReducerClass(WordCountReducer.class);
        // 指定mapper类输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 指定mapper类输出value类型
        job.setMapOutputValueClass(IntWritable.class);
        // 指定mr最终输出的key类型
        job.setOutputKeyClass(Text.class);
        // 指定mr最终输出的value类型
        job.setOutputValueClass(IntWritable.class);
        // 指定mr数据的输入路径
        // FileInputFormat.setInputPaths(job, "/wordcount/input");
        FileInputFormat.setInputPaths(job, "C:/Users/Administrator/Desktop/input");
        // 指定mr数据的输出路径
        // FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/Administrator/Desktop/output"));
        // 提交程序，并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
