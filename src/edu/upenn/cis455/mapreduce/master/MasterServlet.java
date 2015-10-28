package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  private HashMap<String,WorkerStatus>workerstatusMap;

  public void init(){
	  workerstatusMap = new HashMap<>();
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response){
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  System.out.println(pathInfo);
	  if (pathInfo.equals("/status/newjob")){
		  for (String key : workerstatusMap.keySet()){
			  String ip = key.split(":")[0];
			  int port = Integer.parseInt(key.split(":")[1]);
			  try {
				Socket socket = new Socket(ip, port);
				OutputStream out = socket.getOutputStream();
				out.write("POST /worker/runmap HTTP/1.0".getBytes());
				out.flush();
				out.close();
				socket.close();
			} catch (IOException e) {
		
				e.printStackTrace();
			}
			  
		  }
	  }
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  System.out.println(pathInfo);
	  if (pathInfo.equals("/workerstatus")){
		  
		  long timestamp = new Date().getTime();
		  WorkerStatus ws = new WorkerStatus(request, timestamp);
		  workerstatusMap.put(ws.getIpPort(), ws);		
		  html = "<html> Done </html>";
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
  
