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

	public Mapper(WordCount job, LinkedList<String> queue, int numWorkers, String spoolOut) {
		this.job = job;
		this.queue = queue;
		this.numWorkers = numWorkers;
		this.spoolOut = spoolOut;
	}

	public void run() {

		System.out.println("Started " + Thread.currentThread().getName());
		
		while (true) {
			synchronized (queue) {
				System.out.println(queue);
				if (queue.isEmpty()) {
					try {
						System.out.println("Waiting");
						queue.wait();

					} catch (InterruptedException e) {
						System.out.println("Thread interrupted while waiting" + WorkerServlet.getStop());
						if (WorkerServlet.getStop()){
							break;
						}

					}

				}
 
				else {
					String line = queue.remove(0);
					String key = line.split("\t")[0];
					String value = line.split("\t")[1];
					MapContext context = new MapContext(numWorkers, spoolOut);
					job.map(key,  value,  context);

				}
			}
			System.out.println("mapper : inside while");
		}

	}
}
