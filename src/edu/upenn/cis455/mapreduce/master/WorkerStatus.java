package edu.upenn.cis455.mapreduce.master;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

public class WorkerStatus {
	private HttpServletRequest request;
	private long timestamp;
	private String port;
	private String status;
	private String job;
	private String keysRead;
	private String keysWritten;
	private String ip;
	
	public WorkerStatus(HttpServletRequest request, long timestamp){
		this.timestamp = timestamp;
		this.request = request;
		parseWorkerStatus();
		
		
	}
	
	public void parseWorkerStatus(){
		port = request.getParameter("port");
		System.out.println("Port of worker: " + port);
		status = request.getParameter("status");
		job = request.getParameter("job");
		keysRead = request.getParameter("keysRead");
		keysWritten = request.getParameter("keysWritten");
		ip = request.getRemoteAddr();
	}
	
	public String getPort(){
		
		return port;
		
	}
	
	public String getIP(){
		return ip;
		
	}
	
	public String getJob(){
		return job;
	}
	
	public String getStatus(){
		return status;
	}
	
	public String getKeysRead(){
		return keysRead;
	}
	
	public String getKeysWritten(){
		return keysWritten;
	}
	
	public long getTimestamp(){
		return timestamp;
	}
	
	public String getIpPort(){
		String uniqueKey = ip +":" + port;
		return uniqueKey;
	}
	
}
