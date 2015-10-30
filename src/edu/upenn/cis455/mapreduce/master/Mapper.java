package edu.upenn.cis455.mapreduce.master;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;
import edu.upenn.cis455.mapreduce.job.WordCount;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class Mapper extends Thread {
	private Job job;
	private LinkedList<String> queue;

	public Mapper(WordCount job, LinkedList<String> queue) {
		this.job = job;
		this.queue = queue;
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
						System.out.println("Thread interrupted while waiting");

					}

				}
 
				else {
					String line = queue.remove(0);
					String key = line.split("\t")[0];
					String value = line.split("\t")[1];
					MapContext context = new MapContext();
					job.map(key,  value,  context);
					
					if (queue.isEmpty() && WorkerServlet.getStop())
						break;
				}
			}
			System.out.println("mapper : inside while");
		}

	}
}
