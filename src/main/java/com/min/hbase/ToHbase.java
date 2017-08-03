package com.min.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class ToHbase {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		Text kText = new Text();
		IntWritable vWritable = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			for (String string : split) {
				kText.set(string);
				context.write(kText, vWritable);
			}
		}
	}

	public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable i : arg1) {
				sum += i.get();
			}
			Put put = new Put(arg0.getBytes());
			put.addColumn(Bytes.toBytes("age"), Bytes.toBytes("sum"), Bytes.toBytes(sum));
			arg2.write(new ImmutableBytesWritable(Bytes.toBytes("age")), put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ToHbase.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		TableMapReduceUtil.initTableReducerJob("students", Reduce.class, job, null, null, null, null, false);

		FileInputFormat.addInputPath(job, new Path("/users/test.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
