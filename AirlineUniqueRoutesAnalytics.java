/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.airline.analytics;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

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
public class AirlineUniqueRoutesAnalytics extends Configured implements Tool {

	public static class AirlineMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static HashMap<String, String> airportMap = new HashMap<String, String>();
		private BufferedReader brReader;

		private String splitBy = ",";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);

			URI[] files = context.getCacheFiles(); // getCacheFiles returns null

			Path filePath = new Path("codes.csv");

			System.out.println(">>>> File Path : " + filePath.toString());

			loadAirportHashMap(filePath, context);

			/* if (filePath.toString().trim().equals("/airline/codes.csv")) {
							loadAirportHashMap(filePath, context);
			} */
		}

		private void loadAirportHashMap(Path filePath, Context context) throws IOException {

			System.out.println(">>>> inside load airport hash map");

			String strLineRead = "";

			try {

				System.out.println("Load Map File Path : "+ filePath.toString());

				brReader = new BufferedReader(new FileReader(filePath.toString()));

				//System.out.println(">>>> Line : "  + brReader.readLine());

				// Read each line, split and load to HashMap
				while ((strLineRead = brReader.readLine()) != null) {

					String airportFieldArray[] = strLineRead.split(splitBy);

					// System.out.println(">>>>> orign dest : " + airportFieldArray[0].trim() + "  " + airportFieldArray[1].trim());

					airportMap.put(airportFieldArray[0].trim(), airportFieldArray[1].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (brReader != null) {
					brReader.close();
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			IntWritable one = new IntWritable(1);
			Text airports = new Text();

			String[] column = value.toString().split(splitBy);

			// System.out.println(">>>>> map orign dest string  : " + column[16] + " " + column[16]);

			String orignAirport = airportMap.get(column[16]);
			String destinationAirport = airportMap.get(column[17]);

			// System.out.println(">>>>> map orign dest : " + orignAirport + " " + destinationAirport);

			airports.set(orignAirport + "," + destinationAirport);
			context.write(airports, one);
		}
	}

	public static class AirlineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] strings) throws Exception {

		Job job = Job.getInstance(getConf(), "Hadoop Airline Orign Destination Analytics");

		job.setJarByClass(getClass());

		// Distributed Cache
		job.addCacheFile(new URI("/airline/codes.csv"));

		job.setMapperClass(AirlineMapper.class);
		// job.setCombinerClass(AirlineReducer.class);
		job.setReducerClass(AirlineReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(strings[0]));
		FileOutputFormat.setOutputPath(job, new Path(strings[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AirlineUniqueRoutesAnalytics(), args);
		System.exit(exitCode);
	}
}
