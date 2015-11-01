package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.Context;

public class MapContext implements Context{
	private static BigInteger maxSize = new BigInteger("1461501637330902918203684832716283019655932542975");
	private int numWorkers;
	private String spoolOut;
	public MapContext(int numWorkers, String spoolOut){
		this.numWorkers = numWorkers;
		this.spoolOut = spoolOut;
	}
	
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
			String[] words = value.split(" ");
			String line ;
			System.out.println("Writing into file " + filename);
			for (String word : words){
				line = word + "\t" + "1\n";
				fw.append(line);

			}
			fw.close();
	
		} catch (IOException e) {
			System.out.println("Error in writing to file");
			e.printStackTrace();
		}
		
	}
	public void write(String key, String value){
		System.out.println("Chilling in context");
		BigInteger blockSize = maxSize.divide(BigInteger.valueOf(numWorkers));
		BigInteger hashedKey =  shaHash(key).divide(blockSize);		
		int workerNum = hashedKey.intValue() + 1;
		String filename = spoolOut +"/worker" + workerNum;
		writeToFile(filename, key, value);
	
		
	}
	
	
}
