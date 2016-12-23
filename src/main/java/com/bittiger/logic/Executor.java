package com.bittiger.logic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;
import com.bittiger.client.Utilities;

public class Executor extends Thread {

	private ClientEmulator c;

	public Executor(ClientEmulator c) {
		this.c = c;
	}

	private static transient final Logger LOG = LoggerFactory
			.getLogger(Executor.class);

	@Override
	public void run() {
		//Executor executors the events in the queue one by one.
		LOG.info("Executor starts......");
		while (true) {
			ActionType actionType = c.getEventQueue().peek();
			long currTime = System.currentTimeMillis();
			if (currTime > c.getStartTime() + c.getTpcw().warmup
					+ c.getTpcw().mi) {
				return;
			}
			try {
				LOG.info(actionType + " request received");
				if (actionType == ActionType.AvailNotEnoughAddServer
						|| actionType == ActionType.BadPerformanceAddServer) {
					if (c.getLoadBalancer().getCandidateQueue().size() == 0) {
						LOG.info("CandidateQueue size is 0, skip adding server");
					} else {
						Server target = c.getLoadBalancer().getCandidateQueue().remove(0);
						Server source = c.getLoadBalancer().getReadQueue()
								.get(c.getLoadBalancer().getReadQueue().size() - 1);
						Server master = c.getLoadBalancer().getWriteQueue();
						// make sure source ! = master
						if(source.equals(master)){
							LOG.error("source should not be equal to master");
							continue;
						}
						scaleOut(source.getIp(), target.getIp(), master.getIp());
						c.getLoadBalancer().addServer(target);
						LOG.info("kick in " + target.getIp() + " done ");
					}
				} else if (actionType == ActionType.GoodPerformanceRemoveServer) {
					if (c.getLoadBalancer().getReadQueue().size() == Utilities.minimumSlave) {
						LOG.info("Read queue size is " + Utilities.minimumSlave
								+ ", skip scale in");
					} else {
						Server server = c.getLoadBalancer().removeServer();
						scaleIn(server.getIp());
						LOG.info("Kick out server" + server.getIp() + " done ");
					}
				}
				LOG.info(actionType + " request done");
				//now consume the token
				c.getEventQueue().get();
			} catch (Exception e) {
				LOG.error(e.getMessage());
			}
		}
	}

	public boolean scaleOut(String source, String target, String master)
			throws InterruptedException, IOException {
		ProcessBuilder pb = new ProcessBuilder("/bin/bash",
				"script/callScaleOut.sh", source, target, master);
		Process p = pb.start();
		LOG.info("Kick in " + target + " from " + source);
		BufferedReader is = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		while ((line = is.readLine()) != null)
			LOG.info(line);
		p.waitFor();
		return true;
	}

	public boolean scaleIn(String target) throws InterruptedException,
			IOException {
		ProcessBuilder pb = new ProcessBuilder("/bin/bash",
				"script/callScaleIn.sh", target);
		Process p = pb.start();
		LOG.info("Kick out " + target);
		BufferedReader is = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		while ((line = is.readLine()) != null)
			LOG.info(line);
		p.waitFor();
		return true;
	}
}
