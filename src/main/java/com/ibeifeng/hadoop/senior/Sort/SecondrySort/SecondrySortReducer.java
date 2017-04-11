package com.ibeifeng.hadoop.senior.Sort.SecondrySort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondrySortReducer extends Reducer<SecondrySortPair, IntWritable, Text, IntWritable>{
    private static final Logger logger = LoggerFactory.getLogger(SecondrySortReducer.class);
    private static final Text SEPARATOR = new Text("----------");
    private Text outKey = new Text();
    @Override
    protected void reduce(SecondrySortPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        logger.info("-------reduce阶段key.getFirst()----------"+  key.getFirst()+"");
        logger.info("-------reduce阶段key.getSecond()----------"+  key.getSecond()+"");
        context.write(SEPARATOR, null);
        outKey.set(Integer.toString(key.getFirst()));
        for (IntWritable val : values) {
            context.write(outKey, val);
            logger.info("-------reduce阶段输出Key----------"+ outKey+"");
            logger.info("-------reduce阶段输出value----------"+ val+"");
        }
    }
}