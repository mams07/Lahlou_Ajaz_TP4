package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerNumTreeSpe extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable res = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int i = 0;
        for (IntWritable val : values) {
            i += val.get();
        }
        res.set(i);
        context.write(key, res);
    }
}
