package com.min.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

//增删改
public class HbaseOp {
	static Configuration conf = HBaseConfiguration.create();
	/*
	 * 添加conf的客户端和服务端： 1.在项目的path下添加hbase-site.xml文件 在配置文件里写入配置信息
	 * 在代码里直接加载配置文件，可以被自动扫描到 2.直接通过Configuration.set（“key”，“value”）设置配置信息
	 * 
	 * hbase的客户端连接服务端需要配置：（“key”,“value”）
	 * hbase.zookeeper.quorum：hbase的zookeeper的主机名
	 * hbase.zookeeper.property.clientPort：服务端口号（默认2181）
	 */

	// 创建表
	public static void create() throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		// 用户admin执行代码
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		String tableName = "students";
		if (admin.tableExists(tableName)) {
			System.out.println("table already exits");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			// 列族
			HColumnDescriptor columnDesc1 = new HColumnDescriptor("name");
			HColumnDescriptor columnDesc2 = new HColumnDescriptor("age");

			tableDesc.addFamily(columnDesc1);
			tableDesc.addFamily(columnDesc2);

			admin.createTable(tableDesc);
		}
	}

	// put
	public static void insert() throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("students"));

		Put put = new Put(Bytes.toBytes("5"));
		put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("fristname"), Bytes.toBytes("qqq"));
		put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("sencondname"), Bytes.toBytes("www"));
		put.addColumn(Bytes.toBytes("age"), null, Bytes.toBytes("33"));
		Put put2 = new Put(Bytes.toBytes("6"));

		put2.addColumn(Bytes.toBytes("name"), Bytes.toBytes("fristname"), Bytes.toBytes("aaa"));
		put2.addColumn(Bytes.toBytes("name"), Bytes.toBytes("sencondname"), Bytes.toBytes("sss"));
		put2.addColumn(Bytes.toBytes("age"), null, Bytes.toBytes("28"));

		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		puts.add(put2);

		table.put(puts);
	}

	public static void update() throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("students"));

		Put put = new Put(Bytes.toBytes("1"));
		put.addColumn(Bytes.toBytes("age"), null, Bytes.toBytes(28));

		table.put(put);
	}

	public static void delete() throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("students"));

		Delete del = new Delete(Bytes.toBytes("2"));

		table.delete(del);
	}

	public static void select() throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("students"));
		List<Get> gets = new ArrayList<Get>();

		gets.add(new Get(Bytes.toBytes("1")));
		gets.add(new Get(Bytes.toBytes("2")));

		Result[] results = table.get(gets);
		if (results != null && results.length > 0) {
			for (Result result : results) {
				if (!result.isEmpty()) {
					for (Cell cell : result.listCells()) {
						// cell:列族、列、值
						// 列族
						System.out.print(
								Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
						// 列
						System.out.print(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
								cell.getQualifierLength()));
						// 值
						System.out.print(
								Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					}
				}
				System.out.println();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// create();
		insert();
		// update();
		 //delete();
		//select();
	}
}
