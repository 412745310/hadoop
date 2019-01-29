package com.chelsea.hadoop.mapreduce.flowSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.Data;

import org.apache.hadoop.io.WritableComparable;

/**
 * 流量排序实体类
 * 
 * @author shevchenko
 *
 */
@Data
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    
    @Override
    public void readFields(DataInput in) throws IOException {
        long upFlow = in.readLong();
        long downFlow = in.readLong();
        long sumFlow = in.readLong();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    @Override
    public int compareTo(FlowSortBean o) {
        return Integer.parseInt(String.valueOf(o.getSumFlow() - this.getSumFlow()));
    }
    
}
