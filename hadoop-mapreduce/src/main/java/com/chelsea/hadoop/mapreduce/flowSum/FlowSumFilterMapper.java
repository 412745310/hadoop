package com.chelsea.hadoop.mapreduce.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 流量统计过滤mapper
 * 
 * @author shevchenko
 *
 */
public class FlowSumFilterMapper extends Mapper<Text, FlowSumBean, Text, LongWritable>{
    
    private LongWritable outValue = new LongWritable();
    
    @Override
    protected void map(Text key, FlowSumBean value, Mapper<Text, FlowSumBean, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        if (value.getSumFlow() >= 60) {
            outValue.set(value.getSumFlow());
            context.write(key, outValue);
        }
    }

}
