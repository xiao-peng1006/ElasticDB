package com.bittiger.dbserver;

import java.sql.Connection;

public abstract class Server {
	String ip;

	public abstract Connection getConnection() throws Exception;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

}
