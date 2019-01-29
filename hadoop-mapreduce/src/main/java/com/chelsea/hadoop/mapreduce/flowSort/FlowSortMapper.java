package com.chelsea.hadoop.mapreduce.flowSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 流量排序mapper
 * 
 * @author shevchenko
 *
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {

    private FlowSortBean flowBean = new FlowSortBean();
    private Text text = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowSortBean, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);
        text.set(phoneNum);
        context.write(flowBean, text);
    }
    
}
