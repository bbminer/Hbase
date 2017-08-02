package com.min.hbase;

import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public class MD5 {
	public static byte[] md5(String input) throws Exception {
		// 初始化 并返回二进制数据
		return MessageDigest.getInstance("MD5").digest(Bytes.toBytes(input));
	}

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(md5("wang")));
	}
}