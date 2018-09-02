package com.bittiger.misc;

import java.sql.Connection;
import java.sql.Statement;

import com.bittiger.dbserver.HiveServer;
import com.bittiger.dbserver.Server;
import com.bittiger.querypool.QueryMetaData;

public class HiveStandAlone {
	/*
	 *
	 * Before Running this example we should start thrift server. To Start Thrift
	 * server we should run below command in terminal hive --service hiveserver2 &
	 * Please replace the following HIVE_SERVER_IP with the EC2 host's ip.
	 */
	public static String HIVE_SERVER_IP = "18.237.178.176";

	public static void main(String[] args) throws Exception {

		Server server = new HiveServer(HIVE_SERVER_IP);

		Connection connection = server.getConnection();

		Statement stmt = connection.createStatement();

		for (int index = 1; index <= 13; index++) {
			String queryclass = "bq" + index;
			String classname = "com.bittiger.querypool." + queryclass;
			QueryMetaData query = (QueryMetaData) Class.forName(classname).newInstance();
			String command = query.getQueryStr();
			System.out.println("execute:" + command);
			long start = System.currentTimeMillis();
			try {
				stmt.executeQuery(command);
			} catch (Exception e) {
				System.out.println("error:" + e.getMessage());
			}
			long end = System.currentTimeMillis();
			System.out.println("time:" + (end - start));
		}

		for (int index = 1; index <= 5; index++) {
			String queryclass = "wq" + index;
			String classname = "com.bittiger.querypool." + queryclass;
			QueryMetaData query = (QueryMetaData) Class.forName(classname).newInstance();
			String command = query.getQueryStr();
			System.out.println("execute:" + command);
			long start = System.currentTimeMillis();
			try {
				stmt.executeUpdate(command);
			} catch (Exception e) {
				System.out.println("error:" + e.getMessage());
			}
			long end = System.currentTimeMillis();
			System.out.println("time:" + (end - start));
		}
		stmt.close();
		connection.close();
	}
}
