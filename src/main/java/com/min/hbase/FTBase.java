package com.min.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FTBase {
	public static class Map extends TableMapper<ImmutableBytesWritable, Put> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Cell cell = value.getColumnLatestCell(Bytes.toBytes("age"), Bytes.toBytes("sum"));
			byte[] row = CellUtil.cloneValue(cell); // 读取name的值
			byte[] row2 = value.getRow(); // rowKey
			ImmutableBytesWritable oWritable = new ImmutableBytesWritable(row);
			context.write(oWritable, new Put(row2));
		}
	}

	public static class Reduce extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable arg0, Iterable<Put> arg1,
				Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Mutation>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Put put : arg1) {
				Put p = new Put(arg0.get());
				p.addColumn(Bytes.toBytes("age"), arg0.get(), Bytes.toBytes(5));
				arg2.write(arg0, p);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FTBase.class);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("age"), Bytes.toBytes("sum"));
		TableMapReduceUtil.initTableMapperJob("students", scan, Map.class, ImmutableBytesWritable.class, Put.class, job,
				false);
		TableMapReduceUtil.initTableReducerJob("studentd", Reduce.class, job, null, null, null, null, false);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
