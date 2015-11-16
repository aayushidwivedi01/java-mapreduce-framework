package edu.upenn.cis455.mapreduce.master;

import java.util.Date;
import java.util.HashMap;

/**
 * The Class HtmlPages.
 * Creates HTML pages for MasterServlet
 */
public class HtmlPages {
	
	/**
	 * Status page.
	 *
	 * @param map the map
	 * @return the string
	 */
	public static String statusPage(HashMap<String, WorkerStatus> map){
		StringBuilder html = new StringBuilder();
		html.append("<html><h3>Map Reduce Status Page : Aayushi Dwivedi : aayushi</br>"
				+ "</h3><h3> Worker Status</h3>");
		html.append("<table border = \"1\" style = \"width:100%\" >" );
		html.append("<tr>");
		html.append("<td>Worker IP:port</td>");
		html.append("<td>Job</td>");
		html.append("<td>Status</td>");
		html.append("<td>Num keys read</td>");
		html.append("<td>Num keys written</td>");
		html.append("</tr>");
		synchronized(map){
			for (String key : map.keySet()){
				WorkerStatus ws = map.get(key);
				long timestamp = ws.getTimestamp();
				long currTime = new Date().getTime();
				if (currTime - timestamp < 30000){
					html.append("<tr>");
					html.append("<td>"+ ws.getIpPort() +"</td>");
					html.append("<td>"+ ws.getJob() +"</td>");
					html.append("<td>"+ ws.getStatus() +"</td>");
					html.append("<td>" + ws.getKeysRead() + "</td>");
					html.append("<td>"+ ws.getKeysWritten() + "</td>");
					html.append("</tr>");
				}
				
				
			}
		}
		
		
		html.append("</table>");
		html.append("</br></br>");
		html.append("<form action=\"/master/status/newjob\" method =\"post\"><h3> Job Form </h3><label>Class name of the job:</label></br> ");
		html.append("<input type = \"text\" name = \"job\"/></br></br><label>Input directory:</label> </br> ");
		html.append("<input type = \"text\" name = \"input\"/></br></br><label>Output directory:</label></br>  ");
		html.append("<input type = \"text\" name = \"output\"/></br></br><label>No. of map threads:</label> </br> ");
		html.append("<input type = \"text\" name = \"numMap\"/></br></br><label>No. of reduce threads: </label></br>");
		html.append("<input type = \"text\" name = \"numReduce\"/></br>");
		html.append("<input type=\"submit\" value=\"Submit\"></form>");
		html.append("</html>");
		return html.toString();
	}
	
	/**
	 * Run map page.
	 *
	 * @return the string
	 */
	public static String runMapPage(){
		//TO-DO
		StringBuilder html = new StringBuilder();
		
		html.append("<html>Pocessing Job</html>");
		return html.toString();
	}
	
	/**
	 * Busy workers page.
	 *
	 * @return the string
	 */
	public static String busyWorkersPage(){
		StringBuilder html = new StringBuilder();
	
		html.append("<html> Sorry, cannot allocate this job.</br>All workers are busy!</br>");
		html.append("<form action=\"/master/status\" method =\"get\">");
		html.append("<input type=\"submit\" value=\"Back\"></form>");
		html.append("</html>");
		return html.toString();
		
	}
	
	/**
	 * Form run map request.
	 *
	 * @param jobDetails the job details
	 * @return the string
	 */
	public static String formRunMapRequest(JobDetails jobDetails){
		StringBuilder html = new StringBuilder();
		
		html.append("<form action=\"/master/status/newjob\" method =\"post\">");
		html.append("<input type=\"hidden\" name = \"job\" value=\""+jobDetails.getJob()+"\">");
		html.append("<input type=\"hidden\" name = \"input\" value=\""+jobDetails.getInput()+"\">");
		html.append("<input type=\"hidden\" name = \"output\" value=\""+jobDetails.getOutput()+"\">");
		html.append("<input type=\"hidden\" name = \"numMap\" value=\""+jobDetails.getNumMap()+"\">");
		html.append("<input type=\"hidden\" name = \"numReduce\" value=\""+jobDetails.getNumReduce()+"\">");
		html.append("<input type=\"submit\" value=\"Submit\"></form>");
		
		return html.toString();
	}
	

}
