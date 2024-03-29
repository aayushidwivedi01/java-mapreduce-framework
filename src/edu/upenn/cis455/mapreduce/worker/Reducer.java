package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.ReduceContext;
import edu.upenn.cis455.mapreduce.job.WordCount;

// TODO: Auto-generated Javadoc
/**
 * The Class Reducer.
 */
public class Reducer extends Thread{
	
	/** The job. */
	private Job job;
	
	/** The data. */
	private LinkedList<String> data;
	
	/** The output dir. */
	private String outputDIR;
	
	/** The worker servlet. */
	private WorkerServlet workerServlet;
	
	/**
	 * Instantiates a new reducer.
	 *
	 * @param job the job
	 * @param ws the ws
	 */
	public Reducer(Job job, WorkerServlet ws){
		this.workerServlet = ws;
		this.job = job;
		this.data = ws.getReduceData();
		this.outputDIR = ws.getOutputDIR();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run(){
		
		while (true){
			
			synchronized(data){
				if (data.isEmpty()){
					try {
						data.wait();						
					}
					catch (InterruptedException e) {
						if (workerServlet.getStop()){
							break;
						}
					}
				}
				
				else{
					String allLines = data.remove(0);
					String[] lines = allLines.split("\n");
					String key = lines[0].split("\t")[0];
					String[] values = new String[lines.length];
					for (int i = 0 ; i < lines.length ; i++){
						values[i] = lines[i].split("\t")[1];
					}
					ReduceContext context = new ReduceContext(outputDIR, workerServlet.getStatusMap());
				
					job.reduce(key, values, context);
				}
			}
		}
		
	}
}
