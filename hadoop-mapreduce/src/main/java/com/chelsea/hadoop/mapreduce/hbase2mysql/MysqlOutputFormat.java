package com.chelsea.hadoop.mapreduce.hbase2mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.chelsea.hadoop.mapreduce.hbase2mysql.collector.IOutputCollector;
import com.chelsea.hadoop.mapreduce.hbase2mysql.common.GlobalConstants;
import com.chelsea.hadoop.mapreduce.hbase2mysql.util.JdbcManager;

/**
 * 自定义输出到mysql的outputformat类
 * 
 * @author root
 *
 */
public class MysqlOutputFormat extends OutputFormat<Text, Text> {
    private static final Logger logger = Logger.getLogger(MysqlOutputFormat.class);

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
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        logger.debug("reduce-------------> getRecordWriter");
        Configuration conf = context.getConfiguration();
        Connection conn = null;
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("获取数据库连接失败", e);
            throw new IOException("获取数据库连接失败", e);
        }
        return new TransformerRecordWriter(conn, conf);
    }
    
    public class TransformerRecordWriter extends RecordWriter<Text, Text> {
        
        private Connection conn = null;
        private Configuration conf = null;
        private PreparedStatement pstmt = null;
        private Integer batch = 0;
        private String sqlName = "new_install_wordCount";
        private String collectorName = "collector_new_install_wordCount";
        
        public TransformerRecordWriter(Connection conn, Configuration conf) {
            this.conn = conn;
            this.conf = conf;
        }

        /**
         * mapper里面的context.write实际上是将key value输出到这个方法中
         */
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            logger.debug("RecordWriter-------------> write : " + key + "=" + value);
            try {
                if (pstmt == null) {
                    // 使用kpi进行区分，返回sql保存到config中
                    pstmt = this.conn.prepareStatement(conf.get(sqlName));
                }
                batch++;

                String collectorClassName = conf.get(collectorName);
                Class<?> clazz = Class.forName(collectorClassName);
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();
                collector.collect(conf, key, value, pstmt);

                if (batch % Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    batch = 0; // 对应批量计算删除
                }
            } catch (Throwable e) {
                logger.error("在writer中写数据出现异常", e);
                throw new IOException(e);
            }
        }
        
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            logger.debug("RecordWriter-------------> close");
            try {
                this.pstmt.executeBatch();
            } catch (SQLException e) {
                logger.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                try {
                    if (conn != null) {
                        conn.commit(); // 进行connection的提交动作
                    }
                } catch (Exception e) {
                    // nothing
                } finally {
                    try {
                        this.pstmt.close();
                    } catch (SQLException e) {
                        // nothing
                    }
                    if (conn != null)
                        try {
                            conn.close();
                        } catch (Exception e) {
                            // nothing
                        }
                }
            }
        }

    }
    
}
