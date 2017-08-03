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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterCompare {
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

	public static void queryRow() throws Exception {
		Table table = con.getTable(TableName.valueOf("students"));
		// (byte [] startRow, byte [] stopRow) 1 2 (不包含3)
		Scan scan = new Scan(Bytes.toBytes("1"), Bytes.toBytes("3"));
		/*
		 * family qualifier compareOp value
		 */
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("name"), Bytes.toBytes("fristname"),
				CompareOp.EQUAL, Bytes.toBytes("zhang"));
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				System.out.print(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.print(
						Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.print(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
			System.out.println();
		}
	}

	public static void queryColumn() throws Exception {
		Table table = con.getTable(TableName.valueOf("students"));
		Scan scan = new Scan(Bytes.toBytes("1"), Bytes.toBytes("3"));
		// 根据列成员的名称过滤
		// 列族 列成员
		QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("n"));
		// 根据列前缀过滤
		ColumnPrefixFilter filter1 = new ColumnPrefixFilter(Bytes.toBytes("f"));
		// 值过滤
		ValueFilter filter2 = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator("zhang"));
		// 复合过滤器
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		list.addFilter(filter);
		list.addFilter(filter1);
		list.addFilter(filter2);

		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				System.out.print(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.print(
						Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.print(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
			System.out.println();
		}
	}

	public static void main(String[] args) throws Exception {
		// queryRow();
		queryColumn();
	}
}
