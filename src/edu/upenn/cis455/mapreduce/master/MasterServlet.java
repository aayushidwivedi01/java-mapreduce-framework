package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

/**
 * The Class MasterServlet.
 */
public class MasterServlet extends HttpServlet {

  /** The Constant serialVersionUID. */
  static final long serialVersionUID = 455555001;
  
  /** The workerstatus map. */
  private HashMap<String,WorkerStatus>workerstatusMap;
  
  /** The active workers. */
  private ArrayList<String>activeWorkers;
  
  /** The jobs. */
  private LinkedList<JobDetails>jobs;
  
  /* (non-Javadoc)
   * @see javax.servlet.GenericServlet#init()
   */
  public void init(){
	  workerstatusMap = new HashMap<>();
	  activeWorkers = new ArrayList<>();
	  jobs = new LinkedList<>();
  }
  
  /**
   * Update active worker list.
   */
  public void updateActiveWorkerList(){
	  for (String worker : workerstatusMap.keySet()){
		  WorkerStatus ws;
		  synchronized(workerstatusMap){
			 ws = workerstatusMap.get(worker);
		  }
		 
		  long timestamp = ws.getTimestamp();
		  long currTime = (new Date()).getTime();
		  if (currTime - timestamp > 30000 && activeWorkers.contains(worker)){
			  activeWorkers.remove(worker);
		  }
		  else if (!activeWorkers.contains(worker)){
			  System.out.println("BEFORE adding new worker: " + activeWorkers.toString());
			  activeWorkers.add(worker);
			  System.out.println("AFTER adding new worker: " + activeWorkers.toString());

		  }
	  }
  }
  

  /**
   * Gets the map body.
   *
   * @param jobDetails the job details
   * @return the map body
   */
  public String getMapBody(JobDetails jobDetails){
	  StringBuilder body = new StringBuilder();
	  body.append("job="+jobDetails.getJob());
	  body.append("&input="+jobDetails.getInput());
	  body.append("&numThreads="+ jobDetails.getNumMap());
	  body.append("&numWorkers="+ activeWorkers.size());
	  for (int i = 0; i < activeWorkers.size() ; i++){
		  int workerNum = i+1;
		  body.append("&worker"+ workerNum +"="+ activeWorkers.get(i));
	  }
	  return body.toString();  
	  
  }
  
  /**
   * Gets the reduce body.
   *
   * @param jobDetails the job details
   * @return the reduce body
   */
  public String getReduceBody(JobDetails jobDetails){
	  StringBuilder body = new StringBuilder();
	  body.append("job="+jobDetails.getJob());
	  body.append("&output="+jobDetails.getOutput());
	  body.append("&numThreads="+ jobDetails.getNumReduce());
	  return body.toString();  
	  
  }
 
  /**
   * Gets the map status.
   *
   * @return the map status
   */
  private boolean getMapStatus(){
	  boolean done = true;
	  WorkerStatus workerstatus;
	  for (String worker : activeWorkers){
		  System.out.println("WORKER in ACTIVE WORKER LIST : " + worker);
		  synchronized(workerstatusMap){
			  workerstatus = workerstatusMap.get(worker);
		  }
		
		  if (workerstatus.getStatus().equalsIgnoreCase("waiting")){
			  continue;
		  }
		  if (workerstatus.getStatus().equalsIgnoreCase("mapping")){
			  done = false;
			  break;
		  }
	  }
	  
	  return done;
  }
  
  /**
   * Gets the reduce status.
   *
   * @return the reduce status
   */
  private boolean getReduceStatus(){
	  boolean done = true;
	  WorkerStatus workerstatus;
	  for (String worker : activeWorkers){
		  synchronized(workerstatusMap){
			  workerstatus = workerstatusMap.get(worker);
		  }
		  
		  if (workerstatus.getStatus().equalsIgnoreCase("idle")){
			  continue;
		  }
		  if (workerstatus.getStatus().equalsIgnoreCase("reducing")){
			  done = false;
			  break;
		  }
	  }
	  
	  return done;
  }
  
  /**
   * Send post.
   *
   * @param requestType the request type
   * @param body the body
   */
  private void sendPost(String requestType, String body){
	  System.out.println("LIST OF ACTIVE WORKERS BEFORE POST: " + activeWorkers.toString());
	  for (String key : activeWorkers){
		  String ip = key.split(":")[0];
		  int port = Integer.parseInt(key.split(":")[1]);
		  System.out.println("Sending"+  requestType + "to :" + ip + " : " + port);
		  try {
			Socket socket = new Socket(ip, port);
			OutputStream out = socket.getOutputStream();
			String workerURL = "http://"+ key+ requestType;
			String postRequest = "POST " + workerURL + " HTTP/1.0\r\n";
			String contentType = "Content-Type:  application/x-www-form-urlencoded\r\n";
			String contentLen = "Content-Length: "+ body.length() + "\r\n\r\n";
			out.write(postRequest.getBytes());
			out.write(contentType.getBytes());
			out.write(contentLen.getBytes());
			out.write(body.getBytes());
			out.flush();
			out.close();
			socket.close();
		  } catch (IOException e) {
			  System.out.println("Error while writing to worker's socket");
			  e.printStackTrace();
		  }	  
	  }
	  
  }
  
  /* (non-Javadoc)
   * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
   */
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  System.out.println(pathInfo);
	
	  if (pathInfo.equals("/status/newjob")){
		  //check if any job is running
		  JobDetails job= new JobDetails(request);
		  System.out.println("Queueing new job");
		  jobs.add(job);
		  System.out.println("Maping...");
		  html = HtmlPages.runMapPage();
		  jobs.get(0).setStatus("mapping");
		  //Process the first job in queue
		  String body = getMapBody(jobs.get(0));
		  sendPost("/worker/runmap", body);		  
		  //remove job from queue once done
		  
	  } 
	  else {
		  html = "<html>Unkown path</html>";
	  }
	  response.setContentType("text/html");
      response.setContentLength(html.length());

      PrintWriter out = response.getWriter();
      out.write(html);
      response.flushBuffer();
  }
  
  /* (non-Javadoc)
   * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  System.out.println(pathInfo);
	  if (pathInfo.equals("/workerstatus")){
		  long timestamp = new Date().getTime();
		  WorkerStatus ws = new WorkerStatus(request, timestamp);
		  synchronized(workerstatusMap){
			  workerstatusMap.put(ws.getIpPort(), ws);	
		  }
		  
		 
		  updateActiveWorkerList();
		  
		  //check if any jobs are on the job queue
		  if (!jobs.isEmpty()){
			  JobDetails job = jobs.get(0);
			  String jobStatus = job.getStatus();
			 
			  if (jobStatus.equalsIgnoreCase("mapping")){
				  if (getMapStatus()){
					  job.setStatus("reducing");
					  System.out.println("Reducing...");
					  //POST a /runreduce request to workers
					  String body = getReduceBody(jobs.get(0));
					  System.out.println("RUNNNNNNN REDUCEEEE");
					  sendPost("/worker/runreduce", body);
					  
				  }
			  } else if (jobStatus.equals("reducing")){
				  if (getReduceStatus()){
					  jobs.remove(0);
					  System.out.println("Job has been completed");
					  if (!jobs.isEmpty()){
						  jobs.get(0).setStatus("mapping");
						  System.out.println("Dequeing job");
						  System.out.println("Maping...");
						  String body = getMapBody(jobs.get(0));
						  sendPost("/worker/runmap", body);		
					  }
				  }
			  }
		  } else {
			  System.out.println("No jobs on queue");
		  }
		  if (html == null)
			  html = "<html> Submitted </html>";
	  }
	  else if (pathInfo.equals("/status")){
		  html = HtmlPages.statusPage(workerstatusMap);
		  
	  }
	  
	  response.setContentType("text/html");
      response.setContentLength(html.length());

      PrintWriter out = response.getWriter();
      out.write(html);
      response.flushBuffer();

  }
}
  
