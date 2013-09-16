package com.barbdean.examples.wordcountplus;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;


public class WordCountPlus extends Configured implements Tool {

    public static void main(String[] args) throws Exception
    {
       Configuration conf = new Configuration();
       Integer exitCode = ToolRunner.run(new WordCountPlus(),args);
       System.exit(exitCode);
    }


    @Override
    public int run(String[] args) throws Exception {
       Configuration conf = getConf();
 
       Job job = new Job(conf, "wordcountplus");
       job.setJarByClass(WordCountPlus.class);
       job.setMapperClass(WordCountPlusMapper.class);
       job.setReducerClass(WordCountPlusReducer.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       String inputPath = conf.get("wordcountplus.inputPath");
       String outputPath = conf.get("wordcountplus.outputPath");
       FileInputFormat.addInputPath(job,   new Path(inputPath));
       FileOutputFormat.setOutputPath(job, new Path(outputPath));

       return(job.waitForCompletion(true) ? 0 : 1);
    }
}
