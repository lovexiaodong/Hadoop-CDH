package com.zyd.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

public class MyMapper extends TableMapper<Text, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String rowKey = Bytes.toString(key.get());

        Put put = new Put(key.get());

        List<Cell> cells = value.listCells();

        for (Cell cell : cells){
            String family = Bytes.toString(CellUtil.cloneFamily(cell));

            if("cf1".equals(family)){
                put.add(cell);
                context.write(new Text(rowKey), put);
            }

        }
    }
}
