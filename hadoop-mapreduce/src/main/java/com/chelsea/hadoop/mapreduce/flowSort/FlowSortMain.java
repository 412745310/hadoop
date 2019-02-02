package com.chelsea.hadoop.mapreduce.flowSort;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 流量排序运行主类
 * 
 * @author shevchenko
 *
 */
public class FlowSortMain {
    
    private String inputPath;
    private String outputPath;
    
    public FlowSortMain(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static void main(String[] args) throws Exception {
        String inputPath = Thread.currentThread().getContextClassLoader().getResource("").toString() + "file/flowSort";
        String outputPath = "C:/Users/Administrator/Desktop/output/flowSort_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        // 提交程序，并且监控打印程序执行情况
        new FlowSortMain(inputPath, outputPath).getJob().waitForCompletion(true);
    }
    
    public Job getJob() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 指定jar包运行主类
        job.setJarByClass(FlowSortMain.class);
        // 指定mapper类
        job.setMapperClass(FlowSortMapper.class);
        // 指定reducer类
        job.setReducerClass(FlowSortReducer.class);
        // 指定mapper类输出key类型
        job.setMapOutputKeyClass(FlowSortBean.class);
        // 指定mapper类输出value类型
        job.setMapOutputValueClass(Text.class);
        // 指定mr最终输出的key类型
        job.setOutputKeyClass(Text.class);
        // 指定mr最终输出的value类型
        job.setOutputValueClass(FlowSortBean.class);
        // 指定reduce任务数量（默认1个）
        job.setNumReduceTasks(1);
        // 指定mr数据的输入路径
        FileInputFormat.setInputPaths(job, inputPath);
        // 指定mr数据的输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
    
}
