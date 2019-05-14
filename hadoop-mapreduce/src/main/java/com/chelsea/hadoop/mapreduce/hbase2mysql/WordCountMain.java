package com.chelsea.hadoop.mapreduce.hbase2mysql;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.Lists;

/**
 * 单词计算运行主类
 * hbase -> mysql
 * 
 * @author shevchenko
 *
 */
public class WordCountMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "47.107.247.223");
        conf.addResource("transformer-env.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("output-collector.xml");
        Job job = Job.getInstance(conf);
        // 指定jar包运行主类
        job.setJarByClass(WordCountMain.class);
        TableMapReduceUtil.initTableMapperJob(initScans(), WordCountMapper.class, Text.class, Text.class, job, true);
        // 指定自定义输出类
        job.setOutputFormatClass(MysqlOutputFormat.class);
        // 提交程序，并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

    private static List<Scan> initScans() {
        Scan scan = new Scan();
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("wordCount"));
        // 优化设置cache
        scan.setBatch(500);
        scan.setCacheBlocks(true); // 启动cache blocks
        scan.setCaching(1000); // 设置每次返回的行数，默认值100，设置较大的值可以提高速度(减少rpc操作)，但是较大的值可能会导致内存异常。
        return Lists.newArrayList(scan);
    }

}
