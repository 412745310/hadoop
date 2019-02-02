package com.chelsea.hadoop.mapreduce.flowSum;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 流量统计运行主类
 * 
 * @author shevchenko
 *
 */
public class FlowSumMain {
    
    private String inputPath;
    private String outputPath;
    
    public FlowSumMain(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static void main(String[] args) throws Exception {
        String inputPath = Thread.currentThread().getContextClassLoader().getResource("").toString() + "file/flowSum";
        String outputPath = "C:/Users/Administrator/Desktop/output/flowSum_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        // 提交程序，并且监控打印程序执行情况
        new FlowSumMain(inputPath, outputPath).getJob().waitForCompletion(true);
    }
    
    public Job getJob() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, FlowSumReducer.class.getSimpleName());
        // 指定jar包运行主类
        job.setJarByClass(FlowSumMain.class);
        
        // 链式结构
        ChainMapper.addMapper(job, FlowSumMapper.class, LongWritable.class, Text.class, Text.class, FlowSumBean.class, conf);
        ChainReducer.setReducer(job, FlowSumReducer.class, Text.class, FlowSumBean.class, Text.class, FlowSumBean.class, conf);
        ChainReducer.addMapper(job, FlowSumFilterMapper.class, Text.class, FlowSumBean.class, Text.class, Text.class, conf);
       
        // 指定mapper类
        //job.setMapperClass(FlowSumMapper.class);
        // 指定reducer类
        //job.setReducerClass(FlowSumReducer.class);
        
        // 指定mapper类输出key类型
        // job.setMapOutputKeyClass(Text.class);
        // 指定mapper类输出value类型
        //job.setMapOutputValueClass(FlowSumBean.class);
        // 指定mr最终输出的key类型
        // job.setOutputKeyClass(Text.class);
        // 指定mr最终输出的value类型
        // job.setOutputValueClass(LongWritable.class);
        // 指定reduce任务数量（默认1个，有多少个任务数量， 就有多少个分区文件）
        //job.setNumReduceTasks(6);
        job.setNumReduceTasks(1);
        // 指定自定义分区类
        //job.setPartitionerClass(ProvincePartitioner.class);
        // 指定mr数据的输入路径
        FileInputFormat.setInputPaths(job, inputPath);
        // 指定mr数据的输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
    
}
