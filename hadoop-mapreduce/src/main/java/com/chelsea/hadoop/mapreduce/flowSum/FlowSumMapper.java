package com.chelsea.hadoop.mapreduce.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 流量统计mapper
 * 
 * @author shevchenko
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowSumBean> {

    private FlowSumBean flowBean = new FlowSumBean();
    private Text text = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowSumBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        text.set(phoneNum);
        context.write(text, flowBean);
    }

}
