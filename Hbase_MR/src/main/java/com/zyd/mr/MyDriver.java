package com.zyd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDriver extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf());

        job.setJarByClass(MyDriver.class);

        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("hbasetbl1"), new Scan(), MyMapper.class, Text.class, Put.class,job);

        TableMapReduceUtil.initTableReducerJob("hbasetbl2" ,MyReducer.class, job);
        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.101:2181,192.168.1.102:2181,192.168.1.103:2181");

        int res = ToolRunner.run(conf, new MyDriver(), args);
        System.exit(res);
    }

}
