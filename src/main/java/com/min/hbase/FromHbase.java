package com.min.hbase;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FromHbase {
	public static class Map extends TableMapper<Text, Text> {
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context) value:hbase读取的一行
		 */
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Cell cell = value.getColumnLatestCell(Bytes.toBytes("name"), Bytes.toBytes("fristname"));
			kText.set(cell.getRowArray());
			Date date = new Date(cell.getTimestamp());
			vText.set(Bytes.toString(cell.getValueArray()) + date);
			context.write(kText, vText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FromHbase.class);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("fristname"));
		// map添加输入表
		TableMapReduceUtil.initTableMapperJob("students", scan, Map.class, Text.class, Text.class, job, false);
		FileOutputFormat.setOutputPath(job, new Path("/users"));
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
