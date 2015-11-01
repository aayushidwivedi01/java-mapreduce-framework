package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.job.WordCount;

public class Reducer extends Thread{
	private WordCount job;
	public Reducer(WordCount job){
		this.job = job;
	}
	
	public void run(){
		
		
		
	}
}
