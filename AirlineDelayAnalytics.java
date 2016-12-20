/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package com.airline.analytics;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author dhamodharan.ra
 */
public class AirlineDelayAnalytics extends Configured implements Tool {

	public static class AirlineMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		private IntWritable flightNumber = new IntWritable();
		private IntWritable delay = new IntWritable();

		private String splitBy = ",";

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] column = value.toString().split(splitBy);

			flightNumber.set(Integer.parseInt(column[9]));

			int actualElapsedTime = column[11].equals("NA") ? 0 : Integer.parseInt(column[11]);
			int csrElapsedTime = column[11].equals("NA") ? 0 : Integer.parseInt(column[12]);
			int arrivalDelay = column[11].equals("NA") ? 0 : Integer.parseInt(column[14]);
			int depatureDelay = column[11].equals("NA") ? 0 : Integer.parseInt(column[15]);

			int actualDelay = arrivalDelay + depatureDelay + (actualElapsedTime - csrElapsedTime);

			delay.set(actualDelay);

			context.write(flightNumber, delay);
		}
	}

	public static class AirlineReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		private Text result = new Text();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int delay = 0;
			int minDelay = 100000;
			int maxDelay = 0;
			int count = 0;

			for (IntWritable val : values) {

				delay += val.get();

				if (val.get() < minDelay) {
					minDelay = val.get();
				}

				if (val.get() > maxDelay) {
					maxDelay = val.get();
				}
				count++;
			}

			long averageDelay = delay / count;

			if (averageDelay < 0) {
				averageDelay = 0;
			}
			
			if (minDelay < 0){
				minDelay = 0;
			}

			result.set(averageDelay + ", " + minDelay + ", " + maxDelay);

			context.write(key, result);
		}
	}

	@Override
	public int run(String[] strings) throws Exception {

		Job job = Job.getInstance(getConf(), "Hadoop Airline Delay Analytics");

		job.setJarByClass(AirlineDelayAnalytics.class);

		job.setMapperClass(AirlineMapper.class);
		// job.setCombinerClass(AirlineReducer.class);
		job.setReducerClass(AirlineReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(strings[0]));
		FileOutputFormat.setOutputPath(job, new Path(strings[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AirlineDelayAnalytics(), args);
		System.exit(exitCode);
	}
}
