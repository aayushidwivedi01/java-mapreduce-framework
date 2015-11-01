package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

import edu.upenn.cis455.mapreduce.job.ReduceContext;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class Reducer extends Thread{
	private WordCount job;
	private LinkedList<String> data;
	private String outputDIR;
	
	public Reducer(WordCount job, LinkedList<String> data, String outputDIR){
		this.job = job;
		this.data = data;
		this.outputDIR = outputDIR;
	}
	
	public void run(){
		
		while (true){
			
			synchronized(data){
				if (data.isEmpty()){
					try {
						data.wait();						
					}
					catch (InterruptedException e) {
						System.out.println("Thread interrupted while waiting" + WorkerServlet.getStop());
						if (WorkerServlet.getStop()){
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
					System.out.println("calling reducer");
					ReduceContext context = new ReduceContext(outputDIR);
				
					job.reduce(key, values, context);
				}
			}
		}
		
	}
}
