package edu.upenn.cis455.mapreduce.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;

/**
 * The Class ReduceContext.
 * Context class for Reduce
 * Writes key value pair to output file
 */
public class ReduceContext implements Context{
	
	/** The output dir. */
	private static String outputDIR;
	
	/** The status map. */
	private HashMap<String, String> statusMap;
	
	/**
	 * Instantiates a new reduce context.
	 *
	 * @param outputDIR the output dir
	 * @param statusMap the status map
	 */
	public ReduceContext(String output, HashMap<String, String> statusMap){
		outputDIR = output;
		this.statusMap = statusMap;
	}
	
	/**
	 * Update keys written.
	 */
	public void updateKeysWritten(){
		synchronized(statusMap){
			int value = Integer.valueOf(statusMap.get("keysWritten")) + 1;
			statusMap.put("keysWritten", String.valueOf(value));
		}
	}
	
	private static synchronized void syncWrite(String key, String value){
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
			
		} catch (IOException e) {
			System.out.println("Error while writing to output file");
			e.printStackTrace();
		}
	
	}
	/* (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */
	@Override
	public void write(String key, String value) {
			
		syncWrite(key, value);
		updateKeysWritten();
	}
	
	

}
