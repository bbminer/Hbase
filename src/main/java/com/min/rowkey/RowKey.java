package com.min.rowkey;

import java.util.Random;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class RowKey {
	/*
	 * hash md5
	 */
	private long id = 1;
	private long time = System.currentTimeMillis();
	private Random random = new Random();

	/**
	 * 产生rowKey
	 * 
	 * @return
	 */
	public byte[] nectId() {
		byte[] lowTime = Bytes.copy(Bytes.toBytes(time), 4, 4);
		byte[] lowId = Bytes.copy(Bytes.toBytes(id), 4, 4);
		byte[] bsId = Bytes.toBytes(id);
		id++;
		return Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowId, lowTime)).substring(0, 8).getBytes(), bsId);
	}

	public static void main(String[] args) {
		RowKey rowKey = new RowKey();
		byte[] nectId = rowKey.nectId();
		System.out.println(Bytes.toLong(Bytes.copy(nectId, 0, 8)));
		System.out.println(Bytes.toLong(Bytes.copy(nectId, 8, 8)));
	}
}
