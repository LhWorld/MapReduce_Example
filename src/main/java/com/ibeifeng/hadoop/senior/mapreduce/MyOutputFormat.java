package com.ibeifeng.hadoop.senior.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutputFormat extends FileOutputFormat<Text, IntWritable> {
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new MyRecordWriter(fileOut);
    }

    public static class MyRecordWriter extends RecordWriter<Text, IntWritable>{

        private PrintWriter out;
        private String separator ="-";

        public MyRecordWriter(FSDataOutputStream fileOut) {
            out = new PrintWriter(fileOut);
        }

        @Override
        public void write(Text key, IntWritable value) throws IOException,
                InterruptedException {//为了制表规整
            StringBuffer separatorCombine=new StringBuffer();
            for (int i=0;i<45-key.getLength();i++){
                separatorCombine.append(separator);
            }
            out.println(key+ separatorCombine.toString()+value.toString());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            out.close();
        }

    }

}