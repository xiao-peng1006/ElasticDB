package com.bittiger.dbserver;

import java.util.Date;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeTrigger extends TimerTask {
	private static transient final Logger LOG = LoggerFactory.getLogger(TimeTrigger.class);

	public void run() {
		LOG.info("TimeTrigger is running at " + new Date().toString());
		String perf = ElasticDatabase.getInstance().getMonitor().readPerformance();
		if(ElasticDatabase.getInstance().isEnableController()) {
			ElasticDatabase.getInstance().getController().run(perf);
		}
	}

}
