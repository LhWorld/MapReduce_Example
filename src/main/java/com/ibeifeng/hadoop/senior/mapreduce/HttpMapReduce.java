package com.ibeifeng.hadoop.senior.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang.ObjectUtils;
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
import org.junit.Test;
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
			Reducer<Text, Text, Text, IntWritable> {
		private static final Logger logger = LoggerFactory.getLogger(HttpReducer.class);
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();



		@Override
		public void reduce(Text key, Iterable<Text> values,
						   Context context) throws IOException, InterruptedException {
			HashMap<String, IntWritable> map = new HashMap<String, IntWritable>();
			int  flag=0;
			logger.info("----reduce阶段--key----" + key + "");
			for (Text value : values) {
				logger.info("----reduce阶段--value----" + value + "");
				if (!map.containsKey(value.toString())) {
					map.put(value.toString(), new IntWritable(1));
				}
			}
			//String combineKey=key.toString()+"----------"+new IntWritable(map.size()).toString();
			 outputKey.set(key);
			 outputValue.set(map.size());
			 context.write(outputKey,outputValue);
//			StringBuffer temp=new StringBuffer();
//			Iterator<Map.Entry<String, IntWritable>> it = map.entrySet().iterator();
//			logger.info("----reduce阶段--map.size----" + map.size() + "");
//			while (it.hasNext()) {
//				Map.Entry<String, IntWritable> entry = it.next();
//				logger.info("----reduce阶段--entry.getKey()----" + entry.getKey() + "");
//				flag++;
//				if (flag==1) {
//					String combineKey=key.toString()+"----------"+new IntWritable(map.size()).toString();
//					outputKey.set(combineKey);
//					outputValue.set(entry.getKey());
//					temp.append(outputValue+";");
//					context.write(outputKey, outputValue);
//				}else {
//					Text outputKey = new Text();
//					outputValue.set(entry.getKey());
//					temp.append(outputValue+";");
//					context.write(outputKey, new Text(temp.toString()));
//				}
//
//			}

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
		job.setOutputFormatClass(MyOutputFormat.class);

		// 3: set job
		// input  -> map  -> reduce -> output
		// 3.joinFile: input
		Path inPath = new Path("/shuju/*");

		FileInputFormat.addInputPath(job, inPath);

		// 3.2: map
		job.setMapperClass(HttpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 3.3: reduce
		job.setReducerClass(HttpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 3.4: output
//		Path outPath = new Path(args[1]);
//		FileOutputFormat.setOutputPath(job, outPath);
		Random random=new Random();
		int r=random.nextInt(100000);
		SimpleDateFormat bartDateFormat = new SimpleDateFormat
				("HHmmss");
		Date date = new Date();
		System.out.println(bartDateFormat.format(date));
		FileOutputFormat.setOutputPath(job, new Path("/Result/"+bartDateFormat.format(date).toString()+""));

		// 4: submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1 ;

	}

	// step 4: run program
	public static void main(String[] args) throws Exception {
		// joinFile: get confifuration

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://lihu");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("ha.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
		conf.set("yarn.resourcemanager.address", "node2:8032");
		conf.set("mapred.jar", "D:\\stormAction\\HDFS\\classes\\artifacts\\beifeng_hdfs_jar\\beifeng-hdfs.jar");
		conf.set("mapreduce.app-submission.cross-platform", "true");

		// int status = new HttpMapReduce().run(args);
		int status = ToolRunner.run(conf,//
				new HttpMapReduce(),//
				args);

		System.exit(status);
	}

}
