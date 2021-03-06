package com.bittiger.dbserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;
import com.bittiger.misc.Utilities;
import com.bittiger.querypool.CleanStatsQuery;
import com.bittiger.querypool.StatsQuery;

public class Monitor {

	public final Vector<Stats> read = new Vector<Stats>();
	public final Vector<Stats> write = new Vector<Stats>();
	private int seq = 0;
	private int rPos = 0;
	private int wPos = 0;

	private static transient final Logger LOG = LoggerFactory.getLogger(Monitor.class);

	public void initialize() {
		Connection connection = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(
					Utilities.getMySQLStatsUrl(ClientEmulator.getInstance().getTpcw().monitorServer),
					ClientEmulator.getInstance().getTpcw().username, ClientEmulator.getInstance().getTpcw().password);
			connection.setAutoCommit(true);
			stmt = connection.createStatement();
			CleanStatsQuery clean = new CleanStatsQuery();
			stmt.executeUpdate(clean.getQueryStr());
			LOG.info("Clean stats at server " + ClientEmulator.getInstance().getTpcw().monitorServer);
		} catch (Exception e) {
			LOG.error(e.toString());
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}

	private void updateStats(double x, double u, double ur, double uw, double r, double w, double m) {
		Connection connection = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(
					Utilities.getMySQLStatsUrl(ClientEmulator.getInstance().getTpcw().monitorServer),
					ClientEmulator.getInstance().getTpcw().username, ClientEmulator.getInstance().getTpcw().password);
			connection.setAutoCommit(true);
			stmt = connection.createStatement();
			StatsQuery stats = new StatsQuery(x, u, ur, uw, r, w, m);
			stmt.executeUpdate(stats.getQueryStr());
			LOG.info("Stats: Interval:" + x + ", Queries:" + u + ", Read:" + r + ", Write:" + w + ", Nodes:" + m);
		} catch (Exception e) {
			LOG.error(e.toString());
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}

	public void addQuery(int sessionId, String type, long start, long end) {
		// int id = Integer.parseInt(name.substring(name.indexOf("n") + 1));
		Stats stat = new Stats(sessionId, type, start, end);
		if (type.contains("b")) {
			read.add(stat);
		} else {
			write.add(stat);
		}
		LOG.debug(stat.toString());
	}

	public String readPerformance() {
		int rPrevPos = rPos;
		int wPrevPos = wPos;
		rPos = read.size();
		wPos = write.size();
		if (rPrevPos == 0 && wPrevPos == 0) {
			return null;
		}
		StringBuffer perf = new StringBuffer();
		long currTime = System.currentTimeMillis();
		long validStartTime = Math.max(
				ClientEmulator.getInstance().getStartTime() + ClientEmulator.getInstance().getTpcw().warmup,
				currTime - ClientEmulator.getInstance().getTpcw().interval);
		long validEndTime = Math.min(ClientEmulator.getInstance().getStartTime()
				+ ClientEmulator.getInstance().getTpcw().warmup + ClientEmulator.getInstance().getTpcw().mi, currTime);
		long totalTime = 0;
		int count = 0;
		int totCount = 0;
		int rCount = 0;
		int wCount = 0;
		double avgRead = 0.0;
		double avgWrite = 0.0;
		for (int i = rPrevPos; i < rPos; i++) {
			Stats s = read.get(i);
			if ((validStartTime < s.start) && (s.start < validEndTime)) {
				count += 1;
				totalTime += s.duration;
			}
		}
		perf.append("R:" + count + ":" + totalTime);
		if (count > 0) {
			avgRead = totalTime / count;
			perf.append(":" + avgRead);
		} else {
			perf.append(":NA");
		}
		totCount += count;
		rCount = count;

		totalTime = 0;
		count = 0;
		for (int i = wPrevPos; i < wPos; i++) {
			Stats s = write.get(i);
			if ((validStartTime < s.start) && (s.start < validEndTime)) {
				count += 1;
				totalTime += s.duration;
			}
		}
		perf.append(",W:" + count + ":" + totalTime);
		if (count > 0) {
			avgWrite = totalTime / count;
			perf.append(":" + avgWrite);
		} else {
			perf.append(":NA");
		}
		totCount += count;
		wCount = count;

		updateStats(seq++, totCount, rCount, wCount, avgRead, avgWrite,
				ElasticDatabase.getInstance().getLoadBalancer().getReadQueue().size());
		return perf.toString();
	}

}
