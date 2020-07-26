package com.zyd.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyReducer extends TableReducer<Text, Put, Text> {


    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        for (Put put : values){
            context.write(key, put);
        }
    }
}
