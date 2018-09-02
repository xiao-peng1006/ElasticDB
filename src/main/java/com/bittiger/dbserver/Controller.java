package com.bittiger.dbserver;

import java.util.Date;
import org.easyrules.api.RulesEngine;
import org.easyrules.core.RulesEngineBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.logic.rules.ScaleInRule;
import com.bittiger.logic.rules.ScaleOutRule;

public class Controller {
	private static transient final Logger LOG = LoggerFactory.getLogger(Controller.class);

	public void run(String perf) {
		Date date = new Date();
		LOG.info("Controller is running at " + date.toString());
		if (perf != null) {
			/**
			 * Create a rules engine and register the business rule
			 */
			RulesEngine rulesEngine = RulesEngineBuilder.aNewRulesEngine().build();
			ScaleOutRule scaleOutRule = new ScaleOutRule();
			scaleOutRule.setInput(perf);
			ScaleInRule scaleInRule = new ScaleInRule();
			scaleInRule.setInput(perf);
			rulesEngine.registerRule(scaleOutRule);
			rulesEngine.registerRule(scaleInRule);
			rulesEngine.fireRules();
		}
	}
}
