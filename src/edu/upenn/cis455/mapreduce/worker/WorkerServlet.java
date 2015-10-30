package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.WordCount;
import edu.upenn.cis455.mapreduce.master.Mapper;


public class WorkerServlet extends HttpServlet {
  private static HashMap<String, String>statusMap;
  static final long serialVersionUID = 455555002;
  private Mapper[] mapper;
  private LinkedList<String> queue = new LinkedList<>();
  private static boolean stop = false;
  private String spoolOut;
  private String spoolIn;
  
  public void init(){
	  statusMap = new HashMap<>();
	  statusMapInit();
	  String masterPort = getServletConfig().getInitParameter("master");
	  HeartBeat heartBeat = new HeartBeat(masterPort, "8080");
	  heartBeat.start();
  }
  
  public void statusMapInit(){
	  	String port = getServletConfig().getInitParameter("worker");
		statusMap.put("port", port);
		statusMap.put("status", "idle");
		statusMap.put("job", null);
		statusMap.put("keysRead", "0");
		statusMap.put("keysWritten", "0");
  }
  
  public String getSpoolOut(){
	  return spoolOut;
  }
  
  public String getSpoolIn(){
	  return spoolIn;
  }
  public static HashMap<String, String> getStatusMap(){
	  return statusMap;
  }
  public void updateStatus(String status){
	  statusMap.put("status", status);
  }
  public void updateJob(String job){
	  statusMap.put("job", job);
  }
  
  public static boolean getStop(){
	  return stop;
  }
  
  public void generateMappers(int size, WordCount job, LinkedList<String>queue){
	  mapper = new Mapper[size];
	  for (int i = 0; i < size ; i++){
		  mapper[i] = new Mapper(job, queue);
		  mapper[i].setName("Mapper"+i);
		  mapper[i].start();
	  }
  }
  public void readFiles(String name){
	  System.out.println("Dir name: " + name );
	  File dir = new File(name);
	  File[] files = dir.listFiles();
	  BufferedReader br;
	  for (File file : files){
		  try{
			  if (!file.isDirectory()){
				  br = new BufferedReader(new FileReader(file));
				  String line = null;
				  while ((line = br.readLine()) != null){
					  synchronized(queue){
						  queue.add(line);
						  queue.notify();
						  System.out.println("Queue: " + queue.toString()); 
					  }
						  
				  }
			  }
		  }  catch (FileNotFoundException e){
			  System.out.println("Error in opening file ");
		  } catch (IOException e){
			  System.out.println("Error in reading from file");
		  }
			  
	  }	  
	  
	 
  }
  
  public void createSpools(String name){
	  File file = new File(name);
		
		if (file.isDirectory() && file.exists()){
			file.delete();
		}
		
		file.mkdir();
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response){
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  
	  if (pathInfo.equals("/runmap")){
		  System.out.println("Got a new job!");
		  
		  String mapJob = request.getParameter("job");
		  String input = request.getParameter("input");
		  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
		  HashMap<String, String>allWorkers = new HashMap<>();
		  for (int i = 1; i <= numWorkers; i++){
			  String key = "worker"+ i;
			  allWorkers.put(key, request.getParameter(key));
		  }
		  
		  //get input directory
		  input = getServletConfig().getInitParameter("storagedir")
				  + "/" + input;
		  System.out.println("Input Dir:" + input);
		 
		
		  
		  try {
			Class<?> mapClass = Class.forName(mapJob);
			WordCount job = (WordCount)mapClass.newInstance();
			queue = new LinkedList<>();
			spoolOut = getServletConfig().getInitParameter("storagedir") 
					+ "/spoolOut";
			
			spoolIn = getServletConfig().getInitParameter("storagedir") 
					+ "/spoolIn";
			
			createSpools(spoolOut);
			createSpools(spoolIn);
			
			
			
			generateMappers(numThreads, job, queue);
			
			//read all files in input directory
			readFiles(input);
			stop = true;
			for (Thread th : mapper){
				try {
					th.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		  } catch (ClassNotFoundException e) {
			  System.out.println("Job class not found");
			  e.printStackTrace();
		  } catch (IllegalAccessException e){
			  System.out.println("Illegal class access exception");
			  e.printStackTrace();
		  } catch (InstantiationException e){
			  System.out.println("Error while instatiating the Job object");
			  e.printStackTrace();
		  }
		  
		 //generate worker threads
		  
		//update job
		  updateJob(mapJob);
		//update status of the worker
		  updateStatus("mapping");
		  
	  }
	  else if (pathInfo.equals("/runReduce")){
		  String job = request.getParameter("job");
		  String numThreads = request.getParameter("numThreads");
		  String output = request.getParameter("output");
		  
		  output = getServletContext().getInitParameter("storagedir")
				  + "/" + output;
	  }
  }
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<html><head><title>Worker</title></head>");
    out.println("<body>Hi, I am the worker!</body></html>");
  }
}
  
