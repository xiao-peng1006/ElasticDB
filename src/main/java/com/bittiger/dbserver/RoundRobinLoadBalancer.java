package com.bittiger.dbserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RoundRobinLoadBalancer extends LoadBalancer {

	private static transient final Logger LOG = LoggerFactory.getLogger(RoundRobinLoadBalancer.class);

	private int nextReadServer = 0;
	
	@Override
	public synchronized Server getNextReadServer() {
		nextReadServer = (nextReadServer + 1) % readQueue.size();
		Server server = readQueue.get(nextReadServer);
		LOG.debug("choose read server as " + server.getIp());
		return server;
	}

	@Override
	public Server getNextWriteServer() {
		LOG.debug("choose write server as " + writeQueue.getIp());
		return writeQueue;
	}

}
