package edu.upenn.cis455.mapreduce.job;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.upenn.cis455.mapreduce.Context;

public class MapContext implements Context{
	
	private static BigInteger shaHash(String key){
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
	
	public void write(String key, String value){
		System.out.println("Chilling in context");
		
		System.out.println(shaHash(key));
	}
	
	
}
