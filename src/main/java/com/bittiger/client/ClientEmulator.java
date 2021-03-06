package com.bittiger.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bittiger.dbserver.ElasticDatabase;

public class ClientEmulator {

	private static transient final Logger LOG = LoggerFactory.getLogger(ClientEmulator.class);

	private static ClientEmulator clientEmulator;

	// private constructor to force use of
	// getInstance() to create Singleton object
	private ClientEmulator() {
	}

	public static ClientEmulator getInstance() {
		if (clientEmulator == null)
			clientEmulator = new ClientEmulator();
		return clientEmulator;
	}

	@Option(name = "-c", usage = "enable controller")
	private boolean enableController;
	@Option(name = "-d", usage = "enable destroyer")
	private boolean enableDestroyer;
	@Option(name = "-h", usage = "use Hive servers")
	private boolean useHiveServer;
	// receives other command line parameters than options
	@Argument
	private List<String> arguments = new ArrayList<String>();
	private boolean endOfSimulation = false;
	private long startTime;

	OpenSystemTicketProducer producer;

	TPCWProperties tpcw = null;

	private synchronized void setEndOfSimulation() {
		endOfSimulation = true;
		LOG.info("Trigger ClientEmulator.isEndOfSimulation()=" + this.isEndOfSimulation());
	}

	public synchronized boolean isEndOfSimulation() {
		return endOfSimulation;
	}

	public void start(String[] args) {
		ElasticDatabase.getInstance().setClientEmulator(this);

		CmdLineParser parser = new CmdLineParser(this);

		try {
			// parse the arguments.
			parser.parseArgument(args);
			// you can parse additional arguments if you want.
			// parser.parseArgument("more","args");
		} catch (CmdLineException e) {
			// if there's a problem in the command line,
			// you'll get this exception. this will report
			// an error message.
			System.err.println(e.getMessage());
			System.err.println("java ClientEmulator [-c -d -h]");
			// print the list of available options
			parser.printUsage(System.err);
			System.err.println();
			return;
		}

		if (useHiveServer) {
			LOG.info("-h flag is set");
			LOG.info("We will use hive servers");
			tpcw = new TPCWProperties("hive");
			enableController = false;
			enableDestroyer = false;
		}
		// right now, we disable controller when we use hive servers
		else {
			if (enableController) {
				LOG.info("-c flag is set");
				if (enableDestroyer) {
					LOG.info("-d flag is set");
				}
				LOG.info("We will use mysql servers");
			}
			tpcw = new TPCWProperties("tpcw");
		}

		long warmup = tpcw.warmup;
		long mi = tpcw.mi;
		long warmdown = tpcw.warmdown;

		int maxNumSessions = 0;
		int workloads[] = tpcw.workloads;
		for (int i = 0; i < workloads.length; i++) {
			if (workloads[i] > maxNumSessions) {
				maxNumSessions = workloads[i];
			}
		}
		LOG.info("The maximum is : " + maxNumSessions);
		BlockingQueue<Integer> bQueue = new LinkedBlockingQueue<Integer>();

		// Each usersession is a user
		UserSession[] sessions = new UserSession[maxNumSessions];
		for (int i = 0; i < maxNumSessions; i++) {
			sessions[i] = new UserSession(i, this, bQueue);
			sessions[i].holdThread();
			sessions[i].start();
		}

		int currNumSessions = 0;
		int currWLInx = 0;
		int diffWL = 0;

		// producer is for semi-open and open models
		// it shares a bQueue with all the usersessions.
		if (tpcw.mixRate > 0) {
			producer = new OpenSystemTicketProducer(this, bQueue);
			producer.start();
		}

		ElasticDatabase.getInstance().initialize(enableController, enableDestroyer, useHiveServer);

		LOG.info("Client starts......");
		this.startTime = System.currentTimeMillis();
		long endTime = startTime + warmup + mi + warmdown;
		long currTime;
		while (true) {
			currTime = System.currentTimeMillis();
			if (currTime >= endTime) {
				// when it reaches endTime, it ends.
				break;
			}
			diffWL = workloads[currWLInx] - currNumSessions;
			LOG.info("Workload......" + workloads[currWLInx]);
			if (diffWL > 0) {
				for (int i = currNumSessions; i < (currNumSessions + diffWL); i++) {
					sessions[i].releaseThread();
					sessions[i].notifyThread();
				}
			} else if (diffWL < 0) {
				for (int i = (currNumSessions - 1); i >= workloads[currWLInx]; i--) {
					sessions[i].holdThread();
				}
			}
			try {
				LOG.info("Client emulator sleep......" + tpcw.interval);
				Thread.sleep(tpcw.interval);
			} catch (InterruptedException ie) {
				LOG.error("ERROR:InterruptedException" + ie.toString());
			}
			currNumSessions = workloads[currWLInx];
			currWLInx = ((currWLInx + 1) % workloads.length);
		}
		setEndOfSimulation();

		ElasticDatabase.getInstance().finish(enableController, enableDestroyer);

		for (int i = 0; i < maxNumSessions; i++) {
			sessions[i].releaseThread();
			sessions[i].notifyThread();
		}
		LOG.info("Client: Shutting down threads ...");
		for (int i = 0; i < maxNumSessions; i++) {
			try {
				LOG.info("UserSession " + i + " joins.");
				sessions[i].join();
			} catch (java.lang.InterruptedException ie) {
				LOG.error("ClientEmulator: Thread " + i + " has been interrupted.");
			}
		}
		if (tpcw.mixRate > 0) {
			try {
				producer.join();
				LOG.info("Producer joins");
			} catch (java.lang.InterruptedException ie) {
				LOG.error("Producer has been interrupted.");
			}
		}
		LOG.info("Done\n");
		Runtime.getRuntime().exit(0);
	}

	public TPCWProperties getTpcw() {
		return tpcw;
	}

	public void setTpcw(TPCWProperties tpcw) {
		this.tpcw = tpcw;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		ClientEmulator.getInstance().start(args);
	}

}
