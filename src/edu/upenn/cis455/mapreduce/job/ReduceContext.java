package edu.upenn.cis455.mapreduce.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class ReduceContext implements Context{
	private String outputDIR;
	private WorkerServlet workerServlet;
	public ReduceContext(String outputDIR, WorkerServlet ws){
		this.outputDIR = outputDIR;
		this.workerServlet = ws;
	}
	
	@Override
	public void write(String key, String value) {
		String filename = outputDIR + "/output.txt";
		File file = new File (filename);
		if (!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				System.out.println("Error while creating output file");
				e.printStackTrace();
			}
		}
		
		try {
			FileWriter fw = new FileWriter(file, true);
			fw.append(key + "\t" + value + "\n");
			fw.flush();
			fw.close();
			workerServlet.updateKeysWritten();
		} catch (IOException e) {
			System.out.println("Error while writing to output file");
			e.printStackTrace();
		}
		
		
		
	}
	
	

}
