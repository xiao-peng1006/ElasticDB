package com.bittiger.client;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.dbserver.ElasticDatabase;
import com.bittiger.querypool.QueryMetaData;

public class UserSession extends Thread {
	private TPCWProperties tpcw = null;
	private ClientEmulator client = null;
	private Random rand = null;
	private boolean suspendThread = false;
	private BlockingQueue<Integer> queue;
	private int id;

	private static transient final Logger LOG = LoggerFactory.getLogger(UserSession.class);

	public UserSession(int id, ClientEmulator client, BlockingQueue<Integer> bQueue) {
		super("UserSession" + id);
		this.id = id;
		this.queue = bQueue;
		this.client = client;
		this.tpcw = client.getTpcw();
		this.rand = new Random();
	}

	private long TPCWthinkTime(double mean) {
		double r = rand.nextDouble();
		return ((long) (((0 - mean) * Math.log(r))));
	}

	public synchronized void notifyThread() {
		notify();
	}

	public synchronized void releaseThread() {
		suspendThread = false;
	}

	public synchronized void holdThread() {
		suspendThread = true;
	}

	private String computeNextSql(double rwratio, double[] read, double[] write) {
		String sql = "";
		// first decide read or write
		double rw = rand.nextDouble();
		if (rw < rwratio) {
			sql += "bq";
			double internal = rand.nextDouble();
			int num = 0;
			for (int i = 0; i < read.length - 1; i++) {
				if (read[i] < internal && internal <= read[i + 1]) {
					num = i + 1;
					sql += num;
					break;
				}
			}

		} else {
			sql += "wq";
			double internal = rand.nextDouble();
			int num = 0;
			for (int i = 0; i < write.length - 1; i++) {
				if (write[i] < internal && internal <= write[i + 1]) {
					num = i + 1;
					sql += num;
					break;
				}
			}
		}
		return sql;
	}

	public void run() {
		while (!client.isEndOfSimulation()) {
			try {
				synchronized (this) {
					while (suspendThread)
						wait();
				}
				// decide of closed or open system
				double r = rand.nextDouble();
				if (r < tpcw.mixRate) {
					int t = queue.take();
					LOG.debug(t + " has been taken");
				} else {
					Thread.sleep((long) ((float) TPCWthinkTime(tpcw.TPCmean)));
				}
			} catch (Exception ex) {
				LOG.error("Error while running session: " + ex.getMessage());
			}

			String queryclass = computeNextSql(tpcw.rwratio, tpcw.read, tpcw.write);
			String classname = "com.bittiger.querypool." + queryclass;
			try {
				QueryMetaData query = (QueryMetaData) Class.forName(classname).newInstance();
				String command = query.getQueryStr();
				// id is like customer information.
				ElasticDatabase.getInstance().getLoadBalancer().execute(id, queryclass, command);
			} catch (Exception ex) {
				LOG.error("Error while executing query: " + ex.getMessage());
			}
		}
	}
}
