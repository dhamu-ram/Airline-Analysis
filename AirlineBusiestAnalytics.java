package com.airline.analytics;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirlineBusiestAnalytics extends Configured implements Tool{

	public static class AirlineMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private IntWritable one = new IntWritable(1);
		private Text orignAirport = new Text();
		private String splitBy = ",";

		public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException{
					
			String[] column = value.toString().split(this.splitBy);

			this.orignAirport.set(column[16]);
			context.write(this.orignAirport, this.one);
		}
	}

	public static class AirlineReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class IntComparator
	extends WritableComparator
	{
		public IntComparator()
		{
			super();
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			Integer v1 = Integer.valueOf(ByteBuffer.wrap(b1, s1, l1).getInt());
			Integer v2 = Integer.valueOf(ByteBuffer.wrap(b2, s2, l2).getInt());

			return v1.compareTo(v2) * -1;
		}
	}

	public int run(String[] strings)
			throws Exception
	{
		Job job = Job.getInstance(getConf(), "Hadoop Airline Busy Analytics");

		job.setJarByClass(AirlineBusiestAnalytics.class);

		job.setMapperClass(AirlineMapper.class);

		job.setReducerClass(AirlineReducer.class);

		job.setGroupingComparatorClass(IntComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(strings[0]));
		FileOutputFormat.setOutputPath(job, new Path(strings[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception	{
		int exitCode = ToolRunner.run(new AirlineBusiestAnalytics(), args);
		System.exit(exitCode);
	}
}
