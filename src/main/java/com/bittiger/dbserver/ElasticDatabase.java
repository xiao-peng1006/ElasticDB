package com.bittiger.dbserver;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.client.ClientEmulator;

public class ElasticDatabase {

	private static transient final Logger LOG = LoggerFactory.getLogger(ElasticDatabase.class);

	private static ElasticDatabase db;

	ClientEmulator clientEmulator;

	// private constructor to force use of
	// getInstance() to create Singleton object
	private ElasticDatabase() {
	}

	public static ElasticDatabase getInstance() {
		if (db == null)
			db = new ElasticDatabase();
		return db;
	}

	private Timer timer = null;

	public void initialize(boolean enableController, boolean enableDestroyer) {
		if (enableController) {
			this.controller = new Controller();
			timer = new Timer();
			timer.schedule(this.controller, ClientEmulator.getInstance().getTpcw().warmup,
					ClientEmulator.getInstance().getTpcw().interval);
			this.executor = new Executor();
			this.executor.start();
			if (enableDestroyer) {
				destroyer = new Destroyer();
				destroyer.start();
			}
		}
		this.monitor = new Monitor();
		this.monitor.initialize();

		String loadbalancer = "com.bittiger.dbserver." + ClientEmulator.getInstance().getTpcw().loadbalancer
				+ "LoadBalancer";
		try {
			this.loadBalancer = (LoadBalancer) Class.forName(loadbalancer).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void finish(boolean enableController, boolean enableDestroyer) {
		if (enableController) {
			timer.cancel();
			try {
				this.eventQueue.put(ActionType.NoOp);
				executor.join();
				LOG.info("Executor joins");
				if (enableDestroyer) {
					destroyer.join();
					LOG.info("Destroyer joins");
				}
			} catch (java.lang.InterruptedException ie) {
				LOG.error("Executor/Destroyer has been interrupted.");
			}
		}
	}

	/**
	 * The monitor to collect metrics
	 */
	private Monitor monitor;

	/**
	 * The controller that implements control logic
	 */
	private Controller controller;

	/**
	 * The executor that executes commands
	 */
	private Executor executor;

	/**
	 * The destroyer that is used to mimic the server down situation
	 */
	private Destroyer destroyer;

	/**
	 * The loadbalancer that is used to distribute load
	 */
	private LoadBalancer loadBalancer;

	/**
	 * The event queue to coordinate producer and consumer
	 */
	private EventQueue eventQueue = null;

	public Monitor getMonitor() {
		return monitor;
	}

	public void setMonitor(Monitor monitor) {
		this.monitor = monitor;
	}

	public Controller getController() {
		return controller;
	}

	public void setController(Controller controller) {
		this.controller = controller;
	}

	public Executor getExecutor() {
		return executor;
	}

	public void setExecutor(Executor executor) {
		this.executor = executor;
	}

	public Destroyer getDestroyer() {
		return destroyer;
	}

	public void setDestroyer(Destroyer destroyer) {
		this.destroyer = destroyer;
	}

	public LoadBalancer getLoadBalancer() {
		return loadBalancer;
	}

	public void setLoadBalancer(LoadBalancer loadBalancer) {
		this.loadBalancer = loadBalancer;
	}

	public EventQueue getEventQueue() {
		return eventQueue;
	}

	public void setEventQueue(EventQueue eventQueue) {
		this.eventQueue = eventQueue;
	}

	public ClientEmulator getClientEmulator() {
		return clientEmulator;
	}

	public void setClientEmulator(ClientEmulator clientEmulator) {
		this.clientEmulator = clientEmulator;
	}

}
