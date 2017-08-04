package com.min.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixT {
	private Connection con;
	private Statement stat;

	public void getConnection() throws SQLException {
		con = DriverManager.getConnection("jdbc:phoenix:centos131,centos132,centos133:2181");
		stat = con.createStatement();
	}

	public void close() throws SQLException {
		if (stat != null) {
			stat.close();
		}
		if (con != null) {
			con.close();
		}
	}

	public void create() throws SQLException {
		String sql = "create table if not exists customer (id bigint not null,name varchar(20),country varchar(20),constraint c_pk primary key(id))";
		String sql1 = "create table if not exists items (id bigint not null,name varchar(20),price double,constraint i_pk primary key(id))";
		String sql2 = "create table if not exists orders (id bigint not null,c_id bigint,i_id bigint,quantity integer,dt date,constraint o_pk primary key(id))";
		stat.executeUpdate(sql);
		stat.executeUpdate(sql1);
		stat.executeUpdate(sql2);
		con.commit();
	}

	public void insert() throws SQLException {
		stat.executeUpdate("upsert into customer values (1001,'customer1','china')");
		stat.executeUpdate("upsert into customer values (1002,'customer2','usa')");
		stat.executeUpdate("upsert into customer values (1003,'customer3','eng')");

		stat.executeUpdate("upsert into items values (1,'item1',105)");
		stat.executeUpdate("upsert into items values (2,'itemc2',100)");
		stat.executeUpdate("upsert into items values (3,'itemc3',110.5)");

		stat.executeUpdate("upsert into orders values (100,1003,1,111,'2017-5-4')");
		stat.executeUpdate("upsert into orders values (101,1001,3,222,'2017-6-20')");
		stat.executeUpdate("upsert into orders values (102,1001,2,333,'2017-8-10')");
		con.commit();
	}

	public void update() throws SQLException {
		stat.executeUpdate("upsert into items values (2,'item2',100)");
		stat.executeUpdate("upsert into items values (3,'item3',110.5)");
		con.commit();
	}

	public void select() throws SQLException {
		ResultSet resultSet = stat.executeQuery(
				"select c.name,i.name,o.dt from customer c inner join orders o on c.id=o.c_id inner join items i on i.id=o.i_id where c.id=1001");
		while (resultSet.next()) {
			System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " " + resultSet.getDate(3));
		}
		if (resultSet != null) {
			resultSet.close();
		}
	}

	public static void main(String[] args) throws SQLException {
		PhoenixT pT = new PhoenixT();
		pT.getConnection();
		// pT.create();
		// pT.insert();
		// pT.update();

		pT.select();
		pT.close();
	}
}
