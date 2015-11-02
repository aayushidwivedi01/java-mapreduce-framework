package edu.upenn.cis455.mapreduce.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;

public class ReduceContext implements Context{
	private String outputDIR;
	private HashMap<String, String> statusMap;
	
	public ReduceContext(String outputDIR, HashMap<String, String> statusMap){
		this.outputDIR = outputDIR;
		this.statusMap = statusMap;
	}
	
	public void updateKeysWritten(){
		synchronized(statusMap){
			int value = Integer.valueOf(statusMap.get("keysWritten")) + 1;
			statusMap.put("keysWritten", String.valueOf(value));
		}
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
			updateKeysWritten();
		} catch (IOException e) {
			System.out.println("Error while writing to output file");
			e.printStackTrace();
		}
		
		
		
	}
	
	

}
