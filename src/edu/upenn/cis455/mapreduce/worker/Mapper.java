 package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;
import edu.upenn.cis455.mapreduce.job.WordCount;

/**
 * Mapper class performs the actual mapping
 * WorkerServlet instatiantes n num of mappers to 
 * carry out mapping 
 */
public class Mapper extends Thread {
	
	/** The job. */
	private Job job;
	
	/** The queue. */
	private LinkedList<String> queue;
	
	/** The num workers. */
	private int numWorkers;
	
	/** The spool out. */
	private String spoolOut;
	
	/** The worker servlet. */
	private WorkerServlet workerServlet;

	/**
	 * Instantiates a new mapper.
	 *
	 * @param job the job
	 * @param ws the ws
	 */
	public Mapper(Job job, WorkerServlet ws) {
		this.workerServlet = ws;
		this.job = job;
		this.queue = ws.getQueue();
		this.numWorkers = ws.getNumWorkers();
		this.spoolOut = ws.getSpoolOut();
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
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
