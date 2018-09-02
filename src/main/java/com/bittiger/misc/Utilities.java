package com.bittiger.misc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.dbserver.Server;

public class Utilities {
	private static transient final Logger LOG = LoggerFactory.getLogger(Utilities.class);

	public static final int retryTimes = 3;

	public static final int minimumSlave = 3;

	public static String getMySQLUrl(String ip) {
		return "jdbc:mysql://" + ip + "/tpcw?connect_timeout=10";
	}

	public static String getHiveUrl(String ip) {
		// example: "jdbc:hive2://52.40.250.0:10000/default", "hadoop", "");
		return "jdbc:hive2://" + ip + ":10000/default";
	}

	public static String getMySQLStatsUrl(String ip) {
		return "jdbc:mysql://" + ip + "/canvasjs_db";
	}

	public static boolean scaleOut(String source, String target, String master)
			throws InterruptedException, IOException {
		ProcessBuilder pb = new ProcessBuilder("/bin/bash", "scripts/callScaleOut.sh", source, target, master);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		LOG.info("Kick in " + target + " from " + source);
		BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = is.readLine()) != null)
			LOG.info(line);
		p.waitFor();
		return true;
	}

	public static boolean scaleIn(String target) throws InterruptedException, IOException {
		ProcessBuilder pb = new ProcessBuilder("/bin/bash", "scripts/callScaleIn.sh", target);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		LOG.info("Kick out " + target);
		BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = is.readLine()) != null)
			LOG.info(line);
		p.waitFor();
		return true;
	}
}
