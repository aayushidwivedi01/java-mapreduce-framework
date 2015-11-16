package edu.upenn.cis455.mapreduce.master;

import javax.servlet.http.HttpServletRequest;

/**
 * The Class JobDetails.
 * Class holding the details of a job
 */
public class JobDetails {
	
	/** The request. */
	private HttpServletRequest request;
	
	/** The job. */
	private String job;
	
	/** The num map. */
	private String numMap;
	
	/** The num reduce. */
	private String numReduce;
	
	/** The input. */
	private String input;
	
	/** The output. */
	private String output;
	
	/** The status. */
	private String status = "queued";
	
	/**
	 * Instantiates a new job details.
	 *
	 * @param request the request
	 */
	public JobDetails(HttpServletRequest request){
		this.request = request;
		parseRequest();
	}

	/**
	 * Parses the request.
	 */
	private void parseRequest(){
		job = request.getParameter("job");
		numMap = request.getParameter("numMap");
		numReduce = request.getParameter("numReduce");
		input = request.getParameter("input");
		output = request.getParameter("output");
		
	}
	
	/**
	 * Sets the status.
	 *
	 * @param status the new status
	 */
	public void setStatus(String status){
		this.status = status;
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
	 * Gets the job.
	 *
	 * @return the job
	 */
	public String getJob() {
		return job;
	}

	/**
	 * Gets the num map.
	 *
	 * @return the num map
	 */
	public String getNumMap() {
		return numMap;
	}

	/**
	 * Gets the num reduce.
	 *
	 * @return the num reduce
	 */
	public String getNumReduce() {
		return numReduce;
	}

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public String getInput() {
		return input;
	}

	/**
	 * Gets the output.
	 *
	 * @return the output
	 */
	public String getOutput() {
		return output;
	}

	
	
}
