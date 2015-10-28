package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.HashMap;

import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {
  private static HashMap<String, String>statusMap;
  static final long serialVersionUID = 455555002;
  public void init(){
	  statusMap = new HashMap<>();
	  statusMapInit();
	  String masterPort = getServletConfig().getInitParameter("master");
	  HeartBeat heartBeat = new HeartBeat(masterPort, "8080", "else");
	  heartBeat.start();
  }
  
  public void statusMapInit(){
		statusMap.put("port" ,"8080");
		statusMap.put("status", "idle");
		statusMap.put("job", null);
		statusMap.put("keysRead", "0");
		statusMap.put("keysWritten", "0");
  }
  
  public static HashMap<String, String> getStatusMap(){
	  return statusMap;
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response){
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  
	  if (pathInfo.equals("/runmap")){
		  String job = request.getParameter("job");
		  String input = request.getParameter("input");
		  String numThreads = request.getParameter("numThreads");
		  String numWorkers = request.getParameter("numWorkers");
		  String allWorkers = request.getParameter("workers");
		  
		  input = getServletContext().getInitParameter("storagedir")
				  + "/" + input;
		  
		 
		  
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
  
