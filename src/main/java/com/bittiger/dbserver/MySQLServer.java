package com.bittiger.dbserver;

import java.sql.Connection;
import java.sql.DriverManager;

import com.bittiger.client.ClientEmulator;
import com.bittiger.misc.Utilities;

public class MySQLServer extends Server{
	
	public MySQLServer(String ip) {
		this.ip = ip;
	}
	
	public Connection getConnection() throws Exception {
		Connection connection = null;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(
					Utilities.getMySQLUrl(ip),
					ClientEmulator.getInstance().getTpcw().username, ClientEmulator.getInstance().getTpcw().password);
			connection.setAutoCommit(true);
			return connection;
	}

}
