package com.chelsea.hadoop.mapreduce.flowSort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 流量统计reducer
 * 
 * @author shevchenko
 *
 */
public class FlowSortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {

    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> value,
            Reducer<FlowSortBean, Text, Text, FlowSortBean>.Context context) throws IOException, InterruptedException {
        context.write(value.iterator().next(), key);
    }
}
