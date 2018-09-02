package com.bittiger.dbserver;

import java.util.LinkedList;
import java.util.Queue;

import com.bittiger.client.ClientEmulator;

public class EventQueue {
	
	private static EventQueue eventqueue;

	// private constructor to force use of
	// getInstance() to create Singleton object
	private EventQueue() {
	}

	public static EventQueue getInstance() {
		if (eventqueue == null) {
			eventqueue = new EventQueue();
		}
		return eventqueue;
	}
	
	Queue<ActionType> queue = new LinkedList<ActionType>();

	public synchronized ActionType peek() {
		while (queue.isEmpty()) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
		return queue.peek();
	}

	public synchronized void get() {
		queue.poll();
	}

	public synchronized void put(ActionType actionType) {
		switch (actionType) {
		case AvailNotEnoughAddServer:
			// we ignore the add server request if there is already an adding one in the
			// queue
			if (!queue.contains(ActionType.AvailNotEnoughAddServer)
					&& !queue.contains(ActionType.BadPerformanceAddServer)) {
				queue.offer(actionType);
			}
			break;
		case BadPerformanceAddServer:
		case GoodPerformanceRemoveServer:
			// we ignore the performance request if there is anything else going on
			// in the queue
			if (queue.isEmpty()) {
				queue.offer(actionType);
			}
			break;
		case NoOp:
			queue.offer(actionType);
			break;
		default:
			break;
		}
		notifyAll();
	}
}
