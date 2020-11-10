package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class MapperDisCon extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text arr = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
        while (itr.hasMoreTokens()) {
            arr.set(itr.nextToken());
            if (arr.toString().contains(";")) {
                Text arrNum = new Text(arr.toString().split(";")[1]);
                if (!arrNum.equals(new Text("Arrondissements :"))) {
                    context.write(arr, one);
                }
            }
        }
    }
}