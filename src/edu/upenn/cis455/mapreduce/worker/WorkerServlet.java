package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.HashMap;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.master.WorkerThread;

public class WorkerServlet extends HttpServlet {
  private static HashMap<String, String>statusMap;
  static final long serialVersionUID = 455555002;
  private WorkerThread[] workerThread;
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
  
  public static HashMap<String, String> getStatusMap(){
	  return statusMap;
  }
  public void updateStatus(String status){
	  statusMap.put("status", status);
  }
  public void updateJob(String job){
	  statusMap.put("job", job);
  }
  
  public void generateWorkerThreads(int size){
	  workerThread = new WorkerThread[size];
	  for (int i = 0; i < size ; i++){
		  workerThread[i] = new WorkerThread();
		  workerThread[i].start();
	  }
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response){
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  
	  if (pathInfo.equals("/runmap")){
		  System.out.println("Got a new job!");
		  
		  String job = request.getParameter("job");
		  String input = request.getParameter("input");
		  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
		  HashMap<String, String>allWorkers = new HashMap<>();
		  for (int i = 1; i <= numWorkers; i++){
			  String key = "worker"+ i;
			  allWorkers.put(key, request.getParameter(key));
		  }
		  
		  //get input directory
		  input = getServletContext().getInitParameter("storagedir")
				  + "/" + input;
		  System.out.println("Input Dir:" + input);
		 
		  
//		  try {
//			Class mapClass = Class.forName(job);
//			
//			
//		  } catch (ClassNotFoundException e) {
//			  System.out.println("Job class not found");
//			  e.printStackTrace();
//		  }
		  
		 //generate worker threads
		//update job
		  updateJob(job);
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
  
