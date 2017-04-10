package com.ibeifeng.hadoop.senior.Sort;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sort {
    private static final Logger logger = LoggerFactory.getLogger(Sort.class);

    //map将输入中的value化成IntWritable类型，作为输出的key

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static final Logger logger = LoggerFactory.getLogger(Map.class);
        private static IntWritable data = new IntWritable();


        //实现map函数

        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            String line = value.toString();

            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
            logger.info("-------map阶段Key----------"+ key+"");
            logger.info("-------map阶段Value----------"+ value+"");


        }


    }


//reduce将输入中的key复制到输出数据的key上，

//然后根据输入的value-list中元素的个数决定key的输出次数

//用全局linenum来代表key的位次

    public static class Reduce extends

            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public static final Logger logger = LoggerFactory.getLogger(Reduce.class);
        private static IntWritable linenum = new IntWritable(1);


        //实现reduce函数

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
           logger.info("---------reduce阶段--------");
            logger.info("-------Iterable<IntWritable> values----------"+values);
            for (IntWritable val : values) {

                context.write(linenum, key);
                logger.info("-----reduce阶段--linenum----------"+linenum+"");
                logger.info("----reduce阶段--key----"+key+"");
                linenum = new IntWritable(linenum.get() + 1);



            }


        }


    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://lihu");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("ha.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        conf.set("yarn.resourcemanager.address", "node2:8032");
        conf.set("mapred.jar", "D:\\HDFS\\classes\\artifacts\\beifeng_hdfs_jar\\beifeng-hdfs.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        String[] ioArgs = new String[]{"sort_in", "sort_out"};

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Data Sort <in> <out>");

            System.exit(2);

        }
//这一提交本地可以运行
        Job job = new Job(conf, "Data Sort");

        job.setJarByClass(Sort.class);


        //设置Map和Reduce处理类

        job.setMapperClass(Map.class);

        job.setReducerClass(Reduce.class);


        //设置输出类型

        job.setOutputKeyClass(IntWritable.class);

        job.setOutputValueClass(IntWritable.class);


        //设置输入和输出目录

        FileInputFormat.addInputPath(job, new Path("/test/b.txt"));
        Random random=new Random();
        int r=random.nextInt(100000);
        FileOutputFormat.setOutputPath(job, new Path("/test/"+r+""));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}