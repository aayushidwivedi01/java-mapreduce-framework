package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  HashMap<String,WorkerStatus>workerstatusMap = new HashMap<>();

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
//    response.setContentType("text/html");
//    PrintWriter out = response.getWriter();
//    out.println("<html><head><title>Master</title></head>");
//    out.println("<body>Hi, I am the master!</body></html>");
//    out.flush();
//    out.close();
	  String html = null;
	  String pathInfo = request.getPathInfo();
	  
	  if (pathInfo.equals("/master/workerstatus")){
		  
		  long timestamp = new Date().getTime();
		  WorkerStatus ws = new WorkerStatus(request, timestamp);
		  workerstatusMap.put(ws.getIpPort(), ws);		
		  html = "<html> Done </html>";
	  }
	  else if (pathInfo.equals("/master/status")){
		  html = HtmlPages.statusPage(workerstatusMap);
		  
	  }
	  
	  response.setContentType("text/html");
      response.setContentLength(html.length());

      PrintWriter out = response.getWriter();
      out.write(html);
      response.flushBuffer();

  }
}
  
