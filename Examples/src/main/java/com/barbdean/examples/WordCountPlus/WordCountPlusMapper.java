package com.barbdean.examples.wordcountplus;

import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountPlusMapper
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, 
                    Context context) throws IOException, InterruptedException {

      context.getCounter("Mapper","NumberOfMapperCalls").increment(1);

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        context.getCounter("Mapper","LoopCalls").increment(1);
        word.set(itr.nextToken());
        context.write(word, ONE);
      }
   }
}
