package com.ibeifeng.hadoop.senior.Sort.SecondrySort;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondrySortMapper extends Mapper<LongWritable, Text, SecondrySortPair, IntWritable>{
    private static final Logger logger = LoggerFactory.getLogger(SecondrySortMapper.class);
    private SecondrySortPair newKey = new SecondrySortPair();
    private IntWritable outValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        logger.info("-------map阶段key----------"+  key+"");
        logger.info("-------map阶段value----------"+ value+"");
        int first = 0, second = 0;
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while(itr.hasMoreTokens()){
            first = Integer.parseInt(itr.nextToken());
            logger.info("-------map阶段输出first----------"+ first+"");
            if (itr.hasMoreTokens()) {
                second = Integer.parseInt(itr.nextToken());
                logger.info("-------map阶段输出second----------"+ second+"");
            }
            newKey.set(first, second);
            outValue.set(second);
            context.write(newKey, outValue);
            logger.info("-------map阶段输出Key----------"+ newKey+"");
            logger.info("-------map阶段n输出value----------"+ outValue+"");
        }
    }
}