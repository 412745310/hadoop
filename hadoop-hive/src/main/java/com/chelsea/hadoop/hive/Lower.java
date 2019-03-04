package com.chelsea.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 自定义hive函数
 * 
 * @author shevchenko
 *
 */
@SuppressWarnings("deprecation")
public class Lower extends UDF {

    public Text evaluate(Text s) {
        if (s == null) {
            return null;
        }
        return new Text(s.toString().toLowerCase());
    }


}
