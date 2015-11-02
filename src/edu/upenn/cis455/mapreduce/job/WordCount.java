package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  System.out.println("KEY Received by MAP : " + key);
	  context.write(key, value);
	  

  }
  
  public void reduce(String key, String[] values, Context context)
  {
	  System.out.println("KEY Received by REDUCE : " + key);
	 String value = String.valueOf(values.length);
	 context.write(key, value);
	  


  }
  
}
