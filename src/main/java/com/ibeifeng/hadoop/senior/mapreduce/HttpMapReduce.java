package com.ibeifeng.hadoop.senior.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReuce
 * 
 * @author beifeng
 * 
 */
public class HttpMapReduce extends Configured implements Tool{


	// step joinFile: Map Class
	/**
	 * 
	 * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	 */
	public static class HttpMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private static final Logger logger = LoggerFactory.getLogger(HttpMapper.class);
		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			      // line value
			        String lineValue = value.toString();

			      // split
			       String[] strs = lineValue.split(",");
			       if (!strs[12].isEmpty()){
				// get key
				 mapOutputKey.set(strs[12]);
				// set value
			    mapOutputValue.set(strs[0]);
				// output 
				context.write(mapOutputKey, mapOutputValue);
			}
		}

	}

	// step 2: Reduce Class
	/**
	 * 
	 * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	 */
	public static class HttpReducer extends
			Reducer<Text, Text, CombinationKey, Text> {
		private static final Logger logger = LoggerFactory.getLogger(HttpReducer.class);
		private  Text outputValue = new  Text();
		private CombinationKey combinationKey=new CombinationKey();


		@Override
		public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
			HashMap<Text,IntWritable> map=new HashMap<Text,IntWritable>();
			logger.info("----reduce阶段--map----"+map+"");
			logger.info("----reduce阶段--key----"+key+"");
			for(Text value: values){

				if(!map.containsKey(value)){
					map.put(value,  new IntWritable(1));
				}
			}
			combinationKey.setFirstKey(key);
			combinationKey.setSecondKey(new IntWritable(map.size()));
			Iterator<Map.Entry<Text, IntWritable>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			          Map.Entry<Text, IntWritable> entry = it.next();
			logger.info("----reduce阶段--value----"+entry.getKey()+"");
			outputValue.set(entry.getKey());
			context.write(combinationKey, outputValue);
		}
}
	}



	
	// step 3: Driver ,component job
	public int run(String[] args) throws Exception {
		// joinFile: get confifuration
		Configuration configuration = getConf();
		
		// 2: create Job
		Job job = Job.getInstance(configuration, //
				this.getClass().getSimpleName());
		// run jar 
		job.setJarByClass(this.getClass());
		
		// 3: set job 
		// input  -> map  -> reduce -> output
		// 3.joinFile: input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
		
		// 3.2: map
		job.setMapperClass(HttpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 3.3: reduce
		job.setReducerClass(HttpReducer.class);
		job.setOutputKeyClass(CombinationKey.class);
		job.setOutputValueClass(Text.class);
		
		// 3.4: output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		// 4: submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1 ;
		
	}

	// step 4: run program
	public static void main(String[] args) throws Exception {
		// joinFile: get confifuration
		Configuration configuration = new Configuration();
		
		// int status = new HttpMapReduce().run(args);
		int status = ToolRunner.run(configuration,//
				new HttpMapReduce(),//
				args);
		
		System.exit(status);
	}

}
