package com.bittiger.dbserver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.easyrules.api.RulesEngine;
import org.easyrules.core.RulesEngineBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;
import com.bittiger.logic.rules.AvailabilityNotEnoughRule;
import com.bittiger.misc.Utilities;

public abstract class LoadBalancer {
	protected List<Server> readQueue = new ArrayList<Server>();
	protected Server writeQueue = null;
	protected List<Server> candidateQueue = new ArrayList<Server>();
	private static transient final Logger LOG = LoggerFactory.getLogger(LoadBalancer.class);

	public LoadBalancer() {
		if (!ElasticDatabase.getInstance().isUseHiveServer()) {
			writeQueue = new MySQLServer(ClientEmulator.getInstance().getTpcw().writeServer);
			for (int i = 0; i < ClientEmulator.getInstance().getTpcw().readServer.length; i++) {
				readQueue.add(new MySQLServer(ClientEmulator.getInstance().getTpcw().readServer[i]));
			}
			for (int i = 0; i < ClientEmulator.getInstance().getTpcw().candidateServer.length; i++) {
				candidateQueue.add(new MySQLServer(ClientEmulator.getInstance().getTpcw().candidateServer[i]));
			}
		} else {
			writeQueue = new HiveServer(ClientEmulator.getInstance().getTpcw().writeServer);
			for (int i = 0; i < ClientEmulator.getInstance().getTpcw().readServer.length; i++) {
				readQueue.add(new HiveServer(ClientEmulator.getInstance().getTpcw().readServer[i]));
			}
		}
	}

	public void execute(int id, String queryclass, String command) {
		// we use very simple logic to judge if it is write or read query
		Connection connection = null;
		Statement stmt = null;
		try {
			if (command.toLowerCase().startsWith("select")) {
				connection = getNextReadConnection();
				stmt = connection.createStatement();
				long start = System.currentTimeMillis();
				stmt.executeQuery(command);
				long end = System.currentTimeMillis();
				ElasticDatabase.getInstance().getMonitor().addQuery(id, queryclass, start, end);
			} else {
				connection = getNextWriteConnection();
				stmt = connection.createStatement();
				long start = System.currentTimeMillis();
				stmt.executeUpdate(command);
				long end = System.currentTimeMillis();
				ElasticDatabase.getInstance().getMonitor().addQuery(id, queryclass, start, end);
			}
		} catch (Exception ex) {
			LOG.error("Error while executing query: " + ex.getMessage());
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

	// there is only one server in the writequeue and it will never change.
	public abstract Server getNextWriteServer();

	public abstract Server getNextReadServer();

	public synchronized void addServer(Server server) {
		readQueue.add(server);
	}

	public synchronized Server chooseServerToBeRemovedFromReadQueue() {
		Server server = readQueue.remove(readQueue.size() - 1);
		candidateQueue.add(server);
		return server;
	}

	// multiple threads can call this sequentially
	public synchronized boolean removeServer(Server server) {
		return readQueue.remove(server);
	}

	// there is only one server in the writequeue and it will never change.
	public Server getWriteQueue() {
		return writeQueue;
	}

	public synchronized List<Server> getReadQueue() {
		return readQueue;
	}

	// readQueue is shared by the UserSessions and Executor.
	// However, candidateQueue is only called by Executor.
	public List<Server> getCandidateQueue() {
		return candidateQueue;
	}

	public Connection getNextWriteConnection() {
		Server server = getNextWriteServer();
		try {
			return server.getConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Connection getNextReadConnection() {
		Server server = null;
		Connection connection = null;
		while (connection == null) {
			int tryTime = 0;
			server = getNextReadServer();
			while (connection == null && tryTime++ < Utilities.retryTimes) {
				try {
					connection = server.getConnection();
				} catch (Exception e) {
					LOG.error(e.toString());
				}
			}
			if (connection == null) {
				LOG.error("After trying 3 times, we detected that " + server.getIp() + " was down. ");
				if (removeServer(server)) {
					LOG.info("Find " + server.getIp() + " in the list and we call detectFailure.");
					detectFailure();
				} else {
					LOG.info("Can not find " + server.getIp() + " in the list and we skip detectFailure.");
				}
			} else {
				return connection;
			}
		}
		return null;
	}

	public synchronized void detectFailure() {
		/**
		 * Create a rules engine and register the business rule
		 */
		RulesEngine rulesEngine = RulesEngineBuilder.aNewRulesEngine().build();
		AvailabilityNotEnoughRule availabilityRule = new AvailabilityNotEnoughRule();
		availabilityRule.setInput(readQueue.size());
		rulesEngine.registerRule(availabilityRule);
		rulesEngine.fireRules();
	}

}
