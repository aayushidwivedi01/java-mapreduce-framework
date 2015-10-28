package edu.upenn.cis455.mapreduce.master;

import java.util.Date;
import java.util.HashMap;

public class HtmlPages {
	
	public static String statusPage(HashMap<String, WorkerStatus> map){
		StringBuilder html = new StringBuilder();
		html.append("<html><h3> Worker Status</h3>");
		html.append("<table border = \"1\" style = \"width:100%\" >" );
		html.append("<tr>");
		html.append("<td>Worker IP:port</td>");
		html.append("<td>Job</td>");
		html.append("<td>Status</td>");
		html.append("<td>Num keys read</td>");
		html.append("<td>Num keys written</td>");
		html.append("</tr>");
		
		for (String key : map.keySet()){
			WorkerStatus ws = map.get(key);
			long timestamp = ws.getTimestamp()/1000;
			long currTime = new Date().getTime()/1000;
			if (currTime - timestamp < 30){
				html.append("<tr>");
				html.append("<td>"+ ws.getIpPort() +"</td>");
				html.append("<td>"+ ws.getJob() +"</td>");
				System.out.println("Status: " + ws.getStatus());
				html.append("<td>"+ ws.getStatus() +"</td>");
				System.out.println("Status: " + ws.getKeysRead());
				html.append("<td>" + ws.getKeysRead() + "</td>");
				html.append("<td>"+ ws.getKeysWritten() + "</td>");
				html.append("</tr>");
			}
			
			
		}
		
		html.append("</table>");
		html.append("</br></br>");
		html.append("<div><form action = \"post\"> Submit new job </br><label>Class name of the job:</label> ");
		html.append("<input type = \"text\" name = \"job\"/></br><label>Input directory:</label> ");
		html.append("<input type = \"text\" name = \"inputDir\"/></br><label>Output directory:</label> ");
		html.append("<input type = \"text\" name = \"outpuDir\"/></br><label>No. of map threads:</label> ");
		html.append("<input type = \"text\" name = \"numMap\"/></br><label>No. of reduce threads: </label>");
		html.append("<input type = \"text\" name = \"numReduce\"/>");


		 html.append("<input type=\"submit\" value=\"Submit\"></form></div>");

		return html.toString();
	}

}
