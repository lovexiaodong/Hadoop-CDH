package com.zyd.combine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text text = new Text();
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        longWritable.set(1);
        for (String s : split) {
            text.set(s);
            context.write(text, longWritable);
        }
    }
}
