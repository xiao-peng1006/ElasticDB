package com.bittiger.logic.rules;

import org.easyrules.annotation.Action;
import org.easyrules.annotation.Condition;
import org.easyrules.annotation.Rule;

import com.bittiger.dbserver.ActionType;
import com.bittiger.dbserver.EventQueue;

@Rule(name = "ScaleOutRule", description = "Check if we need to add server for better performance")
public class ScaleOutRule {

	private String perf;

	@Condition
	public boolean checkPerformance() {
		String[] tokens = perf.split(",");
		String[] details = tokens[0].split(":");
		return !details[3].equals("NA") && (Double.parseDouble(details[3]) > 400);
	}

	@Action
	public void addServer() throws Exception {
		EventQueue.getInstance().put(ActionType.BadPerformanceAddServer);
	}

	public void setInput(String perf) {
		this.perf = perf;
	}

}
