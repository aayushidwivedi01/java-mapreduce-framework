
package edu.upenn.cis455.mapreduce.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;

/**
 *MapContext.
 *Writes key value pair to files in spool out
 */
public class MapContext implements Context{
	
	/** The max size. */
	private static BigInteger maxSize = new BigInteger("1461501637330902918203684832716283019655932542975");
	
	/** The num workers. */
	private int numWorkers;
	
	/** The spool out. */
	private String spoolOut;
	
	/** The status map. */
	private HashMap<String, String> statusMap;
	
	/**
	 * Instantiates a new map context.
	 *
	 * @param numWorkers the num workers
	 * @param spoolOut the spool out
	 * @param statusMap the status map
	 */
	public MapContext(int numWorkers, String spoolOut, HashMap<String, String> statusMap){
		this.numWorkers = numWorkers;
		this.spoolOut = spoolOut;
		this.statusMap = statusMap;
	}
	
	/**
	 * Sha hash.
	 *
	 * @param key the key
	 * @return the big integer
	 */
	public BigInteger shaHash(String key){
		BigInteger hashedValue = null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			hashedValue = new BigInteger(1, md.digest(key.getBytes("UTF-8")));
			
			
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Unkown Algorithm for SHA hashing");
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.out.println("Unsupported encoding scheme");
			e.printStackTrace();
		}
		
		return hashedValue;
		
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
	
	/**
	 * Write to file.
	 *
	 * @param filename the filename
	 * @param key the key
	 * @param value the value
	 */
	private void writeToFile(String filename, String key, String value){
		File file = new File(filename);
		if (!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				System.out.println("Error while creating new file " + filename);
				e.printStackTrace();
			}
		}
		
		try {
			FileWriter fw = new FileWriter(filename, true);
			fw.append(key + "\t" + value + "\n");
			fw.flush();
			fw.close();
			updateKeysWritten();
	
		} catch (IOException e) {
			System.out.println("Error in writing to file");
			e.printStackTrace();
		}
		
	}
	
	/* (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */
	public void write(String key, String value){
		BigInteger blockSize = maxSize.divide(BigInteger.valueOf(numWorkers));
		BigInteger hashedKey =  shaHash(key).divide(blockSize);		
		int workerNum = hashedKey.intValue() + 1;
		String filename = spoolOut +"/worker" + workerNum;
		writeToFile(filename, key, value);
	
		
	}
	
	
}
