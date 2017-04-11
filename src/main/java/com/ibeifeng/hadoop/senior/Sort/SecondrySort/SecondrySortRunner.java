package com.ibeifeng.hadoop.senior.Sort.SecondrySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SecondrySortRunner extends Configured implements Tool{
    private static final Logger logger = LoggerFactory.getLogger(SecondrySortRunner.class);
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://lihu");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("ha.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        conf.set("yarn.resourcemanager.address", "node2:8032");
        conf.set("mapred.jar", "D:\\HDFS\\classes\\artifacts\\beifeng_hdfs_jar\\beifeng-hdfs.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf, "SecondrySort");
        job.setJarByClass(SecondrySortRunner.class);
        logger.info("===============进入map===============");
        job.setMapperClass(SecondrySortMapper.class);
        logger.info("===============进入reduce===============");
        job.setReducerClass(SecondrySortReducer.class);
        logger.info("===============进入group===============");
        //设置分组函数类，对二次排序非常关键。
        job.setGroupingComparatorClass(GroupingComparator.class);
        logger.info("===============进入MapOutputKeyClass===============");
        //设置Map的输出 key value类，对二次排序非常重要。
        job.setMapOutputKeyClass(SecondrySortPair.class);
        logger.info("===============进入MapOutputValueClass===============");
        job.setMapOutputValueClass(IntWritable.class);

        logger.info("===============进入ReduceOutputValueClass===============");
        job.setOutputKeyClass(Text.class);
        logger.info("===============进入ReduceOutputValueClass===============");
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/SecondSortTest/secondrysort2.txt"));
        Random random=new Random();
        int r=random.nextInt(100000);
        FileOutputFormat.setOutputPath(job, new Path("/ResultSecondSortTest/"+r+""));

        return job.waitForCompletion(true) ? 0:1;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SecondrySortRunner(), args);
        System.exit(res);
    }
}