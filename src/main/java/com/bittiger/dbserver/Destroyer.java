package com.bittiger.dbserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;
import com.bittiger.misc.Utilities;

public class Destroyer extends Thread {

	private static transient final Logger LOG = LoggerFactory.getLogger(Destroyer.class);

	@Override
	public void run() {
		// Executor executors the events in the queue one by one.
		LOG.info("Destroyer starts......");
		try {
			Thread.sleep(ClientEmulator.getInstance().getTpcw().interval
					* ClientEmulator.getInstance().getTpcw().destroyerSleepInterval);
			LOG.info("Destroyer destroys " + ClientEmulator.getInstance().getTpcw().destroyTarget);
			Utilities.scaleIn(ClientEmulator.getInstance().getTpcw().destroyTarget);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
