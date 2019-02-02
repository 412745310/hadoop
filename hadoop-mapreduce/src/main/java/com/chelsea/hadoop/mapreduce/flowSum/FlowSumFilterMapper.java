package com.chelsea.hadoop.mapreduce.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 流量统计过滤mapper
 * 
 * @author shevchenko
 *
 */
public class FlowSumFilterMapper extends Mapper<Text, FlowSumBean, Text, Text>{
    
    private Text outValue = new Text();
    
    @Override
    protected void map(Text key, FlowSumBean value, Mapper<Text, FlowSumBean, Text, Text>.Context context)
            throws IOException, InterruptedException {
        if (value.getSumFlow() >= 60) {
            outValue.set(String.valueOf(value.getUpFlow() + "\t" + value.getDownFlow() + "\t" + value.getSumFlow()));
            context.write(key, outValue);
        }
    }

}
