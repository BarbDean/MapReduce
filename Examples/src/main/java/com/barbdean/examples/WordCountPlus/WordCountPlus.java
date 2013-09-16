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
import org.apache.hadoop.fs.FileSystem;


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

       Path inputPath  = new Path(conf.get("wordcountplus.inputPath"));
       Path outputPath = new Path(conf.get("wordcountplus.outputPath"));

       // Hadoop does not let us overwrite, so explicitly remove old output
       FileSystem fs = FileSystem.get(conf);
       if(fs.exists(outputPath)) {
          fs.delete(outputPath);
       }

       FileInputFormat.addInputPath(job,   inputPath);
       FileOutputFormat.setOutputPath(job, outputPath);

       return(job.waitForCompletion(true) ? 0 : 1);
    }
}
