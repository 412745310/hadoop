package com.chelsea.hadoop.mapreduce.hbase2mysql;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * 自定义输出到mysql的outputformat类
 * 
 * @author root
 *
 */
public class TransformerOutputFormat extends OutputFormat<Text, Text> {
    private static final Logger logger = Logger.getLogger(TransformerOutputFormat.class);

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
        logger.debug("reduce-------------> checkOutputSpecs");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        logger.debug("reduce-------------> getOutputCommitter");
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        logger.debug("reduce-------------> getRecordWriter");
        return new TransformerRecordWriter();
    }
    
    public class TransformerRecordWriter extends RecordWriter<Text, Text> {

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            
        }

        @Override
        public void write(Text arg0, Text arg1) throws IOException, InterruptedException {
            
        }
        
    }
    
}
