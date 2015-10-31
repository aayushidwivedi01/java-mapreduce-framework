package edu.upenn.cis455.mapreduce.master;

import javax.servlet.http.HttpServletRequest;

public class JobDetails {
	private HttpServletRequest request;
	private String job;
	private String numMap;
	private String numReduce;
	private String input;
	private String output;
	private String status = "queued";
	public JobDetails(HttpServletRequest request){
		this.request = request;
		parseRequest();
	}

	private void parseRequest(){
		job = request.getParameter("job");
		numMap = request.getParameter("numMap");
		numReduce = request.getParameter("numReduce");
		input = request.getParameter("input");
		output = request.getParameter("output");
		
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public String getStatus(){
		return status;
	}
	
	public String getJob() {
		return job;
	}

	public String getNumMap() {
		return numMap;
	}

	public String getNumReduce() {
		return numReduce;
	}

	public String getInput() {
		return input;
	}

	public String getOutput() {
		return output;
	}

	
	
}
