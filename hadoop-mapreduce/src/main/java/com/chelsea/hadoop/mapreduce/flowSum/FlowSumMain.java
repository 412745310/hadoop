package com.chelsea.hadoop.mapreduce.flowSum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 流量统计运行主类
 * 
 * @author shevchenko
 *
 */
public class FlowSumMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 指定jar包运行主类
        job.setJarByClass(FlowSumMain.class);
        // 指定mapper类
        job.setMapperClass(FlowSumMapper.class);
        // 指定reducer类
        job.setReducerClass(FlowSumReducer.class);
        // 指定mapper类输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 指定mapper类输出value类型
        job.setMapOutputValueClass(FlowSumBean.class);
        // 指定mr最终输出的key类型
        job.setOutputKeyClass(Text.class);
        // 指定mr最终输出的value类型
        job.setOutputValueClass(FlowSumBean.class);
        // 指定reduce任务数量（默认1个）
        job.setNumReduceTasks(1);
        // 指定mr数据的输入路径
        FileInputFormat.setInputPaths(job, Thread.currentThread().getContextClassLoader().getResource("").toString() + "file/flowSum");
        // 指定mr数据的输出路径
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/Administrator/Desktop/output/flowSum"));
        // 提交程序，并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
    
}
