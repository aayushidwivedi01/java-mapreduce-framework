 package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class Mapper extends Thread {
	private Job job;
	private LinkedList<String> queue;
	private int numWorkers;
	private String spoolOut;
	private WorkerServlet workerServlet;

	public Mapper(Job job, WorkerServlet ws) {
		this.workerServlet = ws;
		this.job = job;
		this.queue = ws.getQueue();
		this.numWorkers = ws.getNumWorkers();
		this.spoolOut = ws.getSpoolOut();
	}

	public void run() {		
		while (true) {
			synchronized (queue) {
				if (queue.isEmpty()) {
					try {
						queue.wait();
					} catch (InterruptedException e) {
						if (workerServlet.getStop()){
							break;
						}

					}

				}
 
				else {
					workerServlet.updateKeysRead();
					String line = queue.remove(0);
					//String key = line.split("\t")[0];
					String value = line.split("\t", 2)[1];
					System.out.print("KEY CONTENT before writting: " + value + "\n");
					String[] keys = value.split(" ");
					MapContext context = new MapContext(numWorkers, spoolOut, workerServlet.getStatusMap());
					for (String key : keys){
						job.map(key,  "1",  context);
					}
					

				}
			}
	
		}

	}
}
