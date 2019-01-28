package com.chelsea.hadoop.mapreduce.flowSum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.Data;

import org.apache.hadoop.io.Writable;

/**
 * 流量统计实体类
 * 
 * @author shevchenko
 *
 */
@Data
public class FlowSumBean implements Writable {
    
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
    
}
