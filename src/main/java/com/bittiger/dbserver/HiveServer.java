package com.bittiger.dbserver;

import java.sql.Connection;
import java.sql.DriverManager;

import com.bittiger.misc.Utilities;

public class HiveServer extends Server {

	public static final String HIVE_USERNAME = "hadoop";
	public static final String HIVE_PASSWORD = "";

	public HiveServer(String ip) {
		this.ip = ip;
	}

	public Connection getConnection() throws Exception {
		Connection connection = null;
		Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
		connection = DriverManager.getConnection(Utilities.getHiveUrl(ip), HIVE_USERNAME, HIVE_PASSWORD);
		return connection;
	}

}
