package com.ibeifeng.hadoop.senior.Sort;
import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.*;



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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class STjoin {

    public static final Logger logger = LoggerFactory.getLogger(STjoin.class);

    public static int time = 0;



    /*

     * map将输出分割child和parent，然后正序输出一次作为右表，

     * 反序输出一次作为左表，需要注意的是在输出的value中必须

     * 加上左右表的区别标识。

     */

    public static class Map extends Mapper<Object, Text, Text, Text> {



        // 实现map函数

        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            String childname = new String();// 孩子名称

            String parentname = new String();// 父母名称

            String relationtype = new String();// 左右表标识



            // 输入的一行预处理文本
            logger.info("----map阶段--value----"+value+"");
            StringTokenizer itr=new StringTokenizer(value.toString());

            String[] values=new String[2];

            int i=0;

            while(itr.hasMoreTokens()){

                values[i]=itr.nextToken();

                i++;

            }
            logger.info("----map阶段--values[0]----"+values[0]+"");
            logger.info("----map阶段--values[1]----"+values[1]+"");



            if (values[0].compareTo("child") != 0) {

                childname = values[0];

                parentname = values[1];



                // 输出左表

                relationtype = "1";

                context.write(new Text(values[1]), new Text(relationtype +

                        "+"+ childname + "+" + parentname));

                logger.info("----map阶段输出左表--new Text(relationtype +\n" +
                        "\n" +
                        "                        \"+\"+ childname + \"+\" + parentname)----"+new Text(relationtype +

                        "+"+ childname + "+" + parentname)+"");

                // 输出右表

                relationtype = "2";

                context.write(new Text(values[0]), new Text(relationtype +

                        "+"+ childname + "+" + parentname));
                logger.info("----map阶段输出右表--new Text(relationtype +\n" +
                        "\n" +
                        "                        \"+\"+ childname + \"+\" + parentname)----"+new Text(relationtype +

                        "+"+ childname + "+" + parentname)+"");

            }

        }



    }



    public static class Reduce extends Reducer<Text, Text, Text, Text> {



        // 实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {



            // 输出表头

            if (0 == time) {

                context.write(new Text("grandchild"), new Text("grandparent"));

                time++;
                logger.info("----reuce阶段--values[0]----"+time+"");

            }

            logger.info("----reduce阶段-key----"+key+"");

            int grandchildnum = 0;

            String[] grandchild = new String[10];


            int grandparentnum = 0;

            String[] grandparent = new String[10];




            Iterator ite = values.iterator();

            while (ite.hasNext()) {

                String record = ite.next().toString();
                logger.info("----reduce阶段-record----"+record+"");

                int len = record.length();

                int i = 2;
                logger.info("----reduce阶段-len----"+len+"");
                if (0 == len) {

                    continue;

                }



                // 取得左右表标识

                char relationtype = record.charAt(0);
                logger.info("----reduce阶段-relationtype----"+relationtype+"");

                // 定义孩子和父母变量

                String childname = new String();

                String parentname = new String();



                // 获取value-list中value的child

                while (record.charAt(i) != '+') {

                    childname += record.charAt(i);

                    i++;

                }
                logger.info("----reduce阶段-childname ----"+childname +"");



                i = i + 1;



                // 获取value-list中value的parent

                while (i < len) {

                    parentname += record.charAt(i);

                    i++;
                    logger.info("----reduce阶段- parentname +i得值----"+ parentname+"");

                }
                logger.info("----reduce阶段- parentname ----"+ parentname+"");



                // 左表，取出child放入grandchildren

                if ('1' == relationtype) {

                    grandchild[grandchildnum] = childname;

                    grandchildnum++;
                    logger.info("----reduce阶段- grandchildnum ----"+ grandchildnum+"");

                }



                // 右表，取出parent放入grandparent

                if ('2' == relationtype) {

                    grandparent[grandparentnum] = parentname;

                    grandparentnum++;
                    logger.info("----reduce阶段-  grandparent[grandparentnum] ----"+  grandparent[grandparentnum] +"");
                }

            }



            // grandchild和grandparent数组求笛卡尔儿积

            if (0 != grandchildnum && 0 != grandparentnum) {

                for (int m = 0; m < grandchildnum; m++) {

                    for (int n = 0; n < grandparentnum; n++) {

                        // 输出结果

                        context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                        logger.info("----reduce阶段-new Text(grandchild[m] ----"+new Text(grandchild[m])+"");
                        logger.info("----reduce阶段-new Text(grandparent[n]) ----"+new Text(grandparent[n])+"");

                    }

                }

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


        String[] ioArgs = new String[] { "STjoin_in", "STjoin_out" };

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Single Table Join <in> <out>");

            System.exit(2);

        }



        Job job = new Job(conf, "Single Table Join");

        job.setJarByClass(STjoin.class);



        // 设置Map和Reduce处理类

        job.setMapperClass(Map.class);

        job.setReducerClass(Reduce.class);



        // 设置输出类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);



        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path("/stjoin/file"));
        Random random=new Random();
        int r=random.nextInt(100000);
        SimpleDateFormat bartDateFormat = new SimpleDateFormat
                ("HHmmss");
        Date date = new Date();
        System.out.println(bartDateFormat.format(date));
        FileOutputFormat.setOutputPath(job, new Path("/stjoindddt/"+bartDateFormat.format(date).toString()+""));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}