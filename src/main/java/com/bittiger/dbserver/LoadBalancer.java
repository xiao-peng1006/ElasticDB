package com.bittiger.dbserver;

import java.sql.Connection;
import java.sql.DriverManager;
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
		writeQueue = new Server(ClientEmulator.getInstance().getTpcw().writeQueue);
		for (int i = 0; i < ClientEmulator.getInstance().getTpcw().readQueue.length; i++) {
			readQueue.add(new Server(ClientEmulator.getInstance().getTpcw().readQueue[i]));
		}
		for (int i = 0; i < ClientEmulator.getInstance().getTpcw().candidateQueue.length; i++) {
			candidateQueue.add(new Server(ClientEmulator.getInstance().getTpcw().candidateQueue[i]));
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

	public synchronized void removeServer(Server server) {
		readQueue.remove(server);
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

	public Connection getNextConnection(String sql) {
		// read
		if (sql.contains("b")) {
			return getNextReadConnection();
		} else {
			return getNextWriteConnection();
		}
	}

	public Connection getNextWriteConnection() {
		Server server = getNextWriteServer();
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = (Connection) DriverManager.getConnection(Utilities.getUrl(server),
					ClientEmulator.getInstance().getTpcw().username, ClientEmulator.getInstance().getTpcw().password);
			connection.setAutoCommit(true);
		} catch (Exception e) {
			LOG.error(e.toString());
		}
		LOG.debug("choose write server as " + server.getIp());
		return connection;
	}

	public Connection getNextReadConnection() {
		Server server = null;
		Connection connection = null;
		while (connection == null) {
			int tryTime = 0;
			server = getNextReadServer();
			while (connection == null && tryTime++ < Utilities.retryTimes) {
				try {
					Class.forName("com.mysql.jdbc.Driver").newInstance();
					connection = (Connection) DriverManager.getConnection(Utilities.getUrl(server),
							ClientEmulator.getInstance().getTpcw().username,
							ClientEmulator.getInstance().getTpcw().password);
					connection.setAutoCommit(true);
				} catch (Exception e) {
					LOG.error(e.toString());
				}
			}
			if (connection == null) {
				LOG.error("After trying 3 times, we detected that" + server.getIp() + " is down. ");
				removeServer(server);
				detectFailure();
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
