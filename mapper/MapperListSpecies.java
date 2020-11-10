package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperListSpecies extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text treespe = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
        while (itr.hasMoreTokens()){
            treespe.set(itr.nextToken());
            if(treespe.toString().contains(";")) {
                Text arrNum = new Text(treespe.toString().split(";")[3]);


                if (!arrNum.equals(new Text("Species"))) {

                    context.write(arrNum, one);
                }
            }
        }
    }
}