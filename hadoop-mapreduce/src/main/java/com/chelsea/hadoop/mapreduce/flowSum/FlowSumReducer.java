package com.chelsea.hadoop.mapreduce.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 流量统计reducer
 * 
 * @author shevchenko
 *
 */
public class FlowSumReducer extends Reducer<Text, FlowSumBean, Text, FlowSumBean> {

    private FlowSumBean flowBean = new FlowSumBean();
    
    @Override
    protected void reduce(Text key, Iterable<FlowSumBean> values, Reducer<Text, FlowSumBean, Text, FlowSumBean>.Context context)
            throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow = 0;
        for (FlowSumBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        sumFlow = upFlow + downFlow;
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);
        context.write(key, flowBean);
    }
    
}
