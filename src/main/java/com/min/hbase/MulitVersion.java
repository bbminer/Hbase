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
import org.apache.hadoop.hbase.util.Bytes;

public class MulitVersion {
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
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Table table = con.getTable(TableName.valueOf("students"));
		Scan scan = new Scan();
		scan.setRaw(true);
		scan.setRowPrefixFilter(Bytes.toBytes("1"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> cells = result.getColumnCells(Bytes.toBytes("age"), Bytes.toBytes(""));
			for (Cell cell : cells) {
				System.out.print(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.print(
						Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.print(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
			System.out.println();
		}
	}
}
