package com.min.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbasePage {
	static Configuration conf = HBaseConfiguration.create();
	static Connection con;
	static {
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param num
	 *            每一页显示多少条数据
	 * @param lastRowKey
	 *            数据的rowKey
	 * @return
	 * @throws IOException
	 */
	public static byte[] page(long num, byte[] lastRowKey) throws IOException {
		Table table = con.getTable(TableName.valueOf("students"));
		Scan scan = new Scan();
		Filter filter = new PageFilter(num);
		// 第一页特殊
		if (lastRowKey != null) {
			lastRowKey[lastRowKey.length - 1]++;
			// setStartRow：上包含
			scan.setStartRow(lastRowKey);
		}
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		byte[] bs = null;
		for (Result result : rs) {
			System.out.println("**********");
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				System.out.print(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.print(
						Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
			bs = result.getRow();
		}
		return bs;
	}

	public static void main(String[] args) throws IOException {
		byte[] bs = page(1, null);
		if (bs != null) {
			System.out.println("----------");
			// 接收最近页的数据
			bs = page(2, bs);
		}
	}
}
