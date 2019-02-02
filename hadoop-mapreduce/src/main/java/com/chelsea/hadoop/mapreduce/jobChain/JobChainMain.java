package com.chelsea.hadoop.mapreduce.jobChain;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import com.chelsea.hadoop.mapreduce.flowSort.FlowSortMain;
import com.chelsea.hadoop.mapreduce.flowSum.FlowSumMain;

/**
 * job串行执行主类
 * 
 * @author shevchenko
 *
 */
public class JobChainMain {
    
    public static void main(String[] args) throws Exception {
        String job1InputPath = Thread.currentThread().getContextClassLoader().getResource("").toString() + "file/flowSum";
        String job1OutputPath = "C:/Users/Administrator/Desktop/output/flowSum_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String job2OutputPath = "C:/Users/Administrator/Desktop/output/flowSort_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Job job1 = new FlowSumMain(job1InputPath, job1OutputPath).getJob();
        Job job2 = new FlowSortMain(job1OutputPath, job2OutputPath).getJob();
        ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob1.setJob(job1);
        cJob2.setJob(job2);
        // 设置作业依赖关系
        cJob2.addDependingJob(cJob1);
        JobControl jobControl = new JobControl("JobChainMain");
        jobControl.addJob(cJob1);
        jobControl.addJob(cJob2);
        // 新建一个线程来运行已加入JobControl中的作业，开始进程并等待结束
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(500);
        }
        jobControl.stop();
    }
    
}
