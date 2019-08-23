package com.chelsea.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * hivejdbc测试类
 * 
 * @author shevchenko
 *
 */
public class HiveJdbcClient {

    /**
     * 注意：hive-server2 引用的driver是 org.apache.hive.* 而 hive-server 是 org.apache.hadoop.hive.*
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // hive的默认端口是 10000，如果要修改就修改 hive-site.xml 文件的hive.server2.thrift.port 属性值
        Connection con = DriverManager.getConnection("jdbc:hive2://47.107.247.223:10000/hive", "dev", "aion100111");

        Statement stmt = con.createStatement();
        // 测试的表名 testhivedrivertable
        String tableName = "testhivedrivertable";

        // 如果已经存在就删除
        stmt.execute("drop table if exists " + tableName);

        // 创建这张表
        stmt.execute("create table " + tableName
                + " (key int, value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        // 看下创建是否成功
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // 看下表结构
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // 加载数据到表里面
        // NOTE: filepath 是本地文件所在的位置，注意这个本地不是你的电脑！
        // 你得先把这个文件上传到服务器，然后这里的路径是服务器上这个文件的路径
        // NOTE: /home/dev/tmp/a.txt
        String filepath = "/home/dev/tmp/a.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }
    }

}
