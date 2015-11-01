package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.ReduceContext;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class Reducer extends Thread{
	private Job job;
	private LinkedList<String> data;
	private String outputDIR;
	private WorkerServlet workerServlet;
	public Reducer(Job job, WorkerServlet ws){
		this.workerServlet = ws;
		this.job = job;
		this.data = ws.getReduceData();
		this.outputDIR = ws.getOutputDIR();
	}
	
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
					workerServlet.updateKeysWritten();
					String allLines = data.remove(0);
					
					String[] lines = allLines.split("\n");
					String key = lines[0].split("\t")[0];
					String[] values = new String[lines.length];
					for (int i = 0 ; i < lines.length ; i++){
						values[i] = lines[i].split("\t")[1];
					}
					ReduceContext context = new ReduceContext(outputDIR);
				
					job.reduce(key, values, context);
				}
			}
		}
		
	}
}
