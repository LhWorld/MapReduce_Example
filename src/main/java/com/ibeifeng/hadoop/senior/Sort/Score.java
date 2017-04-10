package com.ibeifeng.hadoop.senior.Sort;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import java.util.Random;
import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Score {

    public static final Logger logger = LoggerFactory.getLogger(Score.class);

    public static class Map extends

            Mapper<LongWritable, Text, Text, IntWritable> {



        // 实现map函数

        public void map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException {

            // 将输入的纯文本文件的数据转化成String

            String line = value.toString();



            // 将输入的数据首先按行进行分割

            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");

            logger.info("----map阶段--tokenizerArticle----"+tokenizerArticle+"");

            // 分别对每一行进行处理

            while (tokenizerArticle.hasMoreElements()) {

                // 每行按空格划分

                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                logger.info("----map阶段空格划分--tokenizerArticle----"+tokenizerArticle+"");



                String strName = tokenizerLine.nextToken();// 学生姓名部分
                logger.info("----map阶段空格划分--strName----"+strName+"");

                String strScore = tokenizerLine.nextToken();// 成绩部分
                logger.info("----map阶段空格划分--strScore----"+strScore+"");



                Text name = new Text(strName);

                int scoreInt = Integer.parseInt(strScore);

                // 输出姓名和成绩

                context.write(name, new IntWritable(scoreInt));

            }

        }



    }



    public static class Reduce extends

            Reducer<Text, IntWritable, Text, IntWritable> {

        // 实现reduce函数

        public void reduce(Text key, Iterable<IntWritable> values,

                           Context context) throws IOException, InterruptedException {



            int sum = 0;

            int count = 0;

            logger.info("----reduce阶段--key----"+key+"");

            Iterator<IntWritable> iterator = values.iterator();

            while (iterator.hasNext()) {

                sum += iterator.next().get();// 计算总分
                logger.info("----reduce阶段--sum----"+sum+"");

                count++;// 统计总的科目数

            }



            int average = (int) sum / count;// 计算平均成绩
            logger.info("----reduce阶段--average----"+average+"");


            context.write(key, new IntWritable(average));

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


        String[] ioArgs = new String[] { "score_in", "score_out" };

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Score Average <in> <out>");

            System.exit(2);

        }



        Job job = new Job(conf, "Score Average");

        job.setJarByClass(Score.class);



        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);

        job.setCombinerClass(Reduce.class);

        job.setReducerClass(Reduce.class);



        // 设置输出类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);



        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现

        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出

        job.setOutputFormatClass(TextOutputFormat.class);



        // 设置输入和输出目录

        FileInputFormat.addInputPath(job, new Path("/score/"));
        Random random=new Random();
        int r=random.nextInt(100000);
        SimpleDateFormat bartDateFormat = new SimpleDateFormat
                ("HHmmss");
        Date date = new Date();
        System.out.println(bartDateFormat.format(date));
        FileOutputFormat.setOutputPath(job, new Path("/scoreResult/"+bartDateFormat.format(date).toString()+""));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    @Test
    public void a(){
        SimpleDateFormat bartDateFormat = new SimpleDateFormat
                ("HHmmss");
        Date date = new Date();
        System.out.println(bartDateFormat.format(date));
    }

}