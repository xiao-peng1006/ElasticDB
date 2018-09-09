package com.bittiger.dbserver;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RandomLoadBalancer extends LoadBalancer {

	private static transient final Logger LOG = LoggerFactory.getLogger(RandomLoadBalancer.class);
	private Random random = new Random();

	@Override
	public synchronized Server getNextReadServer() {
		Server server = readQueue.get(random.nextInt(readQueue.size()));
		LOG.debug("choose read server as " + server.getIp());
		return server;
	}

	@Override
	public Server getNextWriteServer() {
		return writeQueue;
	}

}
