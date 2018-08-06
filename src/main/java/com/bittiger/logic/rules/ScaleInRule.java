package com.bittiger.logic.rules;

import org.easyrules.annotation.Action;
import org.easyrules.annotation.Condition;
import org.easyrules.annotation.Rule;

import com.bittiger.dbserver.ActionType;
import com.bittiger.dbserver.ElasticDatabase;

@Rule(name = "ScaleInRule", description = "Check if we need to remove server for better resource usage")
public class ScaleInRule {

	private String perf;

	@Condition
	public boolean checkPerformance() {
		String[] tokens = perf.split(",");
		String[] details = tokens[0].split(":");
		return !details[3].equals("NA") && (Double.parseDouble(details[3]) < 200 && Double.parseDouble(details[3]) > 0);
	}

	@Action
	public void removeServer() throws Exception {
		ElasticDatabase.getInstance().getEventQueue().put(ActionType.GoodPerformanceRemoveServer);
	}

	public void setInput(String perf) {
		this.perf = perf;
	}

}
