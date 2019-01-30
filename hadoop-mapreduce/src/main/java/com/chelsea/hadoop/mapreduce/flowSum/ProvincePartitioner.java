package com.chelsea.hadoop.mapreduce.flowSum;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 * 
 * @author shevchenko
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowSumBean>{
    
    private static Map<String, Integer> provinceMap = new HashMap<String, Integer>();
    
    static {
        provinceMap.put("131", 0);
        provinceMap.put("132", 1);
        provinceMap.put("133", 2);
        provinceMap.put("134", 3);
        provinceMap.put("135", 4);
    }

    @Override
    public int getPartition(Text key, FlowSumBean value, int arg2) {
        Integer partition = provinceMap.get(key.toString().substring(0, 3));
        if (partition == null) {
            return 5;
        }
        return partition;
    }

}
