package com.bittiger.logic.rules;

import org.easyrules.annotation.Action;
import org.easyrules.annotation.Condition;
import org.easyrules.annotation.Rule;

import com.bittiger.dbserver.ActionType;
import com.bittiger.dbserver.ElasticDatabase;
import com.bittiger.misc.Utilities;

@Rule(name = "AvailabilityRule", description = "Guarrantee the minimum number of slaves")
public class AvailabilityNotEnoughRule {

	private int readQueueSize;

	@Condition
	public boolean checkNumSlave() {
		// The rule should be applied only if
		// the number of slaves is less than minimum.
		return readQueueSize < Utilities.minimumSlave;
	}

	@Action
	public void addServer() throws Exception {
		ElasticDatabase.getInstance().getEventQueue().put(ActionType.AvailNotEnoughAddServer);
	}

	public void setInput(int readQueueSize) {
		this.readQueueSize = readQueueSize;
	}

}
