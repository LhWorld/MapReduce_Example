package com.ibeifeng.hadoop.senior.Sort;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvertedIndex {


    public static final Logger logger = LoggerFactory.getLogger(InvertedIndex.class);
    public static class Map extends Mapper<Object, Text, Text, Text> {

        public static final Logger logger = LoggerFactory.getLogger(Map.class);

        private Text keyInfo = new Text(); // 存储单词和URL组合

        private Text valueInfo = new Text(); // 存储词频

        private FileSplit split; // 存储Split对象



        // 实现map函数

        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            logger.info("----map阶段--key----"+key+"");

            // 获得<key,value>对所属的FileSplit对象

            split = (FileSplit) context.getInputSplit();
            logger.info("----map阶段--split----"+split+"");



            StringTokenizer itr = new StringTokenizer(value.toString());



            while (itr.hasMoreTokens()) {

                // key值由单词和URL组成，如"MapReduce：file1.txt"

                // 获取文件的完整路径

                // keyInfo.set(itr.nextToken()+":"+split.getPath().toString());

                // 这里为了好看，只获取文件的名称。

                int splitIndex = split.getPath().toString().indexOf("file");
                logger.info("----map阶段--splitIndex----"+splitIndex+"");

                keyInfo.set(itr.nextToken() + ":"

                        + split.getPath().toString().substring(splitIndex));

                // 词频初始化为1

                valueInfo.set("1");



                context.write(keyInfo, valueInfo);
                logger.info("----map阶段--keyInfo---"+keyInfo+"");
                logger.info("----map阶段--valueInfo---"+valueInfo+"");

            }

        }

    }



    public static class Combine extends Reducer<Text, Text, Text, Text> {

        public static final Logger logger = LoggerFactory.getLogger(Combine.class);

        private Text info = new Text();



        // 实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            logger.info("----Combine阶段-key----"+key+"");

            // 统计词频

            int sum = 0;

            for (Text value : values) {
                logger.info("----Combine阶段-value----"+value+"");
                sum += Integer.parseInt(value.toString());
                logger.info("----Combine阶段-sum1----"+sum+"");
            }

            logger.info("----Combine阶段-sum2----"+sum+"");

            int splitIndex = key.toString().indexOf(":");
            logger.info("----Combine阶段-splitIndex----"+splitIndex+"");
            // 重新设置value值由URL和词频组成

            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            logger.info("----Combine阶段-info----"+info+"");
            // 重新设置key值为单词

            key.set(key.toString().substring(0, splitIndex));

            logger.info("----Combine阶段- keyEnd----"+ key+"");

            context.write(key, info);

        }

    }



    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public static final Logger logger = LoggerFactory.getLogger(Reduce.class);

        private Text result = new Text();


        // 实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            logger.info("----reduce阶段--key----"+key+"");

            // 生成文档列表

            String fileList = new String();

            for (Text value : values) {

                fileList += value.toString() + ";";
                logger.info("----reduce阶段--value----"+value+"");
                logger.info("----reduce阶段--fileList----"+fileList+"");

            }



            result.set(fileList);



            context.write(key, result);
            logger.info("----reduce阶段-result----"+result+"");

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


        String[] ioArgs = new String[] { "index_in", "index_out" };

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs)

                .getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Inverted Index <in> <out>");

            System.exit(2);

        }



        Job job = new Job(conf, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);



        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);

        job.setCombinerClass(Combine.class);

        job.setReducerClass(Reduce.class);



        // 设置Map输出类型

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);



        // 设置Reduce输出类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);



        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path("/ResverSort/"));
        Random random=new Random();
        int r=random.nextInt(100000);
        SimpleDateFormat bartDateFormat = new SimpleDateFormat
                ("HHmmss");
        Date date = new Date();
        System.out.println(bartDateFormat.format(date));
        FileOutputFormat.setOutputPath(job, new Path("/ResverSortResult/"+bartDateFormat.format(date).toString()+""));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}