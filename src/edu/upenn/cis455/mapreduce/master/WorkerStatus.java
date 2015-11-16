package edu.upenn.cis455.mapreduce.master;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

// TODO: Auto-generated Javadoc
/**
 * The Class WorkerStatus.
 */
public class WorkerStatus {
	
	/** The request. */
	private HttpServletRequest request;
	
	/** The timestamp. */
	private long timestamp;
	
	/** The port. */
	private String port;
	
	/** The status. */
	private String status;
	
	/** The job. */
	private String job;
	
	/** The keys read. */
	private String keysRead;
	
	/** The keys written. */
	private String keysWritten;
	
	/** The ip. */
	private String ip;
	
	/**
	 * Instantiates a new worker status.
	 *
	 * @param request the request
	 * @param timestamp the timestamp
	 */
	public WorkerStatus(HttpServletRequest request, long timestamp){
		this.timestamp = timestamp;
		this.request = request;
		parseWorkerStatus();
		
		
	}
	
	/**
	 * Parses the worker status.
	 */
	public void parseWorkerStatus(){
		port = request.getParameter("port");
		System.out.println("Port of worker: " + port);
		status = request.getParameter("status");
		job = request.getParameter("job");
		keysRead = request.getParameter("keysRead");
		keysWritten = request.getParameter("keysWritten");
		ip = request.getRemoteAddr();
	}
	
	/**
	 * Gets the port.
	 *
	 * @return the port
	 */
	public String getPort(){
		
		return port;
		
	}
	
	/**
	 * Gets the ip.
	 *
	 * @return the ip
	 */
	public String getIP(){
		return ip;
		
	}
	
	/**
	 * Gets the job.
	 *
	 * @return the job
	 */
	public String getJob(){
		return job;
	}
	
	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public String getStatus(){
		return status;
	}
	
	/**
	 * Gets the keys read.
	 *
	 * @return the keys read
	 */
	public String getKeysRead(){
		return keysRead;
	}
	
	/**
	 * Gets the keys written.
	 *
	 * @return the keys written
	 */
	public String getKeysWritten(){
		return keysWritten;
	}
	
	/**
	 * Gets the timestamp.
	 *
	 * @return the timestamp
	 */
	public long getTimestamp(){
		return timestamp;
	}
	
	/**
	 * Gets the ip port.
	 *
	 * @return the ip port
	 */
	public String getIpPort(){
		String uniqueKey = ip +":" + port;
		return uniqueKey;
	}
	
}
