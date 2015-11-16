package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

/**
 * The Class WordCount.
 */
public class WordCount implements Job {

  /* (non-Javadoc)
   * @see edu.upenn.cis455.mapreduce.Job#map(java.lang.String, java.lang.String, edu.upenn.cis455.mapreduce.Context)
   */
  public void map(String key, String value, Context context)
  {
	  System.out.println("KEY Received by MAP : " + key);
	  context.write(key, value);
	  

  }
  
  /* (non-Javadoc)
   * @see edu.upenn.cis455.mapreduce.Job#reduce(java.lang.String, java.lang.String[], edu.upenn.cis455.mapreduce.Context)
   */
  public void reduce(String key, String[] values, Context context)
  {
	  System.out.println("KEY Received by REDUCE : " + key);
	 String value = String.valueOf(values.length);
	 context.write(key, value);
	  


  }
  
}
