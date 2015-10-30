package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  context.write(key, value);

  }
  
  public void reduce(String key, String[] values, Context context)
  {
    // Your reduce function for WordCount goes here


  }
  
}
