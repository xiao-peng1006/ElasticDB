package com.bittiger.dbserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;
import com.bittiger.misc.Utilities;

public class Executor extends Thread {

	private static transient final Logger LOG = LoggerFactory.getLogger(Executor.class);

	@Override
	public void run() {
		// Executor executors the events in the queue one by one.
		LOG.info("Executor starts......");
		while (true) {
			ActionType actionType = ElasticDatabase.getInstance().getEventQueue().peek();
			long currTime = System.currentTimeMillis();
			if (currTime > ClientEmulator.getInstance().getStartTime() + ClientEmulator.getInstance().getTpcw().warmup
					+ ClientEmulator.getInstance().getTpcw().mi) {
				return;
			}
			try {
				LOG.info(actionType + " request received");
				if (actionType == ActionType.AvailNotEnoughAddServer
						|| actionType == ActionType.BadPerformanceAddServer) {
					if (ElasticDatabase.getInstance().getLoadBalancer().getCandidateQueue().size() == 0) {
						LOG.info("CandidateQueue size is 0, skip adding server");
					} else {
						Server target = ElasticDatabase.getInstance().getLoadBalancer().getCandidateQueue().remove(0);
						Server source = ElasticDatabase.getInstance().getLoadBalancer().getReadQueue()
								.get(ElasticDatabase.getInstance().getLoadBalancer().getReadQueue().size() - 1);
						Server master = ElasticDatabase.getInstance().getLoadBalancer().getWriteQueue();
						// make sure source ! = master
						if (source.equals(master)) {
							LOG.error("source should not be equal to master");
							continue;
						}
						Utilities.scaleOut(source.getIp(), target.getIp(), master.getIp());
						ElasticDatabase.getInstance().getLoadBalancer().addServer(target);
						LOG.info("Kick in server " + target.getIp() + " done ");
					}
				} else if (actionType == ActionType.GoodPerformanceRemoveServer) {
					if (ElasticDatabase.getInstance().getLoadBalancer().getReadQueue()
							.size() == Utilities.minimumSlave) {
						LOG.info("Read queue size is " + Utilities.minimumSlave + ", skip scale in");
					} else {
						Server server = ElasticDatabase.getInstance().getLoadBalancer()
								.chooseServerToBeRemovedFromReadQueue();
						Utilities.scaleIn(server.getIp());
						LOG.info("Kick out server " + server.getIp() + " done ");
					}
				}
				LOG.info(actionType + " request done");
				// now consume the event
				ElasticDatabase.getInstance().getEventQueue().get();
			} catch (Exception e) {
				LOG.error(e.getMessage());
			}
		}
	}

}
