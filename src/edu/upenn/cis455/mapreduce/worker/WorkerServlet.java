package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.lang.Thread.State;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.Job;

/**
 * The Class WorkerServlet.
 * Orchestrates the job of map and reduce
 */
public class WorkerServlet extends HttpServlet {
	
	/** The status map. */
	private HashMap<String, String> statusMap;
	
	/** The heart beat. */
	private HeartBeat heartBeat;
	
	/** The num workers. */
	private int numWorkers;
	
	/** The output. */
	private String output;
	
	/** The Constant serialVersionUID. */
	static final long serialVersionUID = 455555002;
	
	/** The mapper. */
	private Mapper[] mapper;
	
	/** The reducer. */
	private Reducer[] reducer;
	
	/** The queue. */
	private LinkedList<String> queue = new LinkedList<>();
	
	/** The stop. */
	private static boolean stop = false;
	
	/** The spool out. */
	private String spoolOut;
	
	/** The spool in. */
	private String spoolIn;
	
	/** The all workers. */
	private HashMap<String, String> allWorkers;
	
	/** The reduce data. */
	private LinkedList<String> reduceData = new LinkedList<>();

	/* (non-Javadoc)
	 * @see javax.servlet.GenericServlet#init()
	 */
	public void init() {
		System.out.println("Initializing worker servlet");

		statusMap = new HashMap<>();
		statusMapInit();
		String masterPort = getServletConfig().getInitParameter("master");
		String workerPort = getServletConfig().getInitParameter("worker");
		heartBeat = new HeartBeat(masterPort, workerPort, this);
		heartBeat.start();
		
	}

	/**
	 * Status map init.
	 */
	public void statusMapInit() {
		String port = getServletConfig().getInitParameter("worker");
		statusMap.put("port", port);
		statusMap.put("status", "idle");
		statusMap.put("job", null);
		statusMap.put("keysRead", "0");
		statusMap.put("keysWritten", "0");
	}

	/**
	 * Gets the spool out.
	 *
	 * @return the spool out
	 */
	public String getSpoolOut() {
		return spoolOut;
	}

	/**
	 * Gets the spool in.
	 *
	 * @return the spool in
	 */
	public String getSpoolIn() {
		return spoolIn;
	}
	
	/**
	 * Gets the queue.
	 *
	 * @return the queue
	 */
	public LinkedList<String> getQueue(){
		return queue;
	}
	
	/**
	 * Gets the num workers.
	 *
	 * @return the num workers
	 */
	public int getNumWorkers(){
		return numWorkers;
	}
	
	/**
	 * Gets the output dir.
	 *
	 * @return the output dir
	 */
	public String getOutputDIR(){
		return output;
	}
	
	/**
	 * Gets the reduce data.
	 *
	 * @return the reduce data
	 */
	public LinkedList<String> getReduceData(){
		return reduceData;
	}
	
	/**
	 * Gets the status map.
	 *
	 * @return the status map
	 */
	public HashMap<String, String> getStatusMap() {
		return statusMap;
	}

	/**
	 * Update status.
	 *
	 * @param status the status
	 */
	public void updateStatus(String status) {
		synchronized(statusMap){
			statusMap.put("status", status);
		}
		
	}
	
	/**
	 * Update keys read.
	 */
	public void updateKeysRead(){
		synchronized(statusMap){
			int value = Integer.valueOf(statusMap.get("keysRead")) + 1;
			statusMap.put("keysRead", String.valueOf(value));
		}
	}

	/**
	 * Update keys written.
	 */
	public void updateKeysWritten(){
		synchronized(statusMap){
			int value = Integer.valueOf(statusMap.get("keysWritten")) + 1;
			statusMap.put("keysWritten", String.valueOf(value));
		}
	}

	/**
	 * Update job.
	 *
	 * @param job the job
	 */
	public void updateJob(String job) {
		synchronized(statusMap){
			statusMap.put("job", job);
		}
		
	}
	
	/**
	 * Reset keys read.
	 */
	public void resetKeysRead(){
		synchronized(statusMap){
			statusMap.put("keysRead", "0");
		}
	}
	
	/**
	 * Reset keys written.
	 */
	public void resetKeysWritten(){
		synchronized(statusMap){
			statusMap.put("keysWritten", "0");
		}
	}
	
	/**
	 * Gets the stop.
	 *
	 * @return the stop
	 */
	public boolean getStop() {
		return stop;
	}

	/**
	 * Generate mappers.
	 *
	 * @param size num of mappers
	 * @param job class
	 */
	public void generateMappers(int size, Job job) {
		mapper = new Mapper[size];
		for (int i = 0; i < size; i++) {
			mapper[i] = new Mapper(job, this);
			mapper[i].setName("Mapper" + i);
			mapper[i].start();
		}
	}
	
	/**
	 * Generate reducers.
	 *
	 * @param size num of reducers
	 * @param job class
	 */
	public void generateReducers(int size , Job job){
		reducer = new Reducer[size];
		
		for (int i = 0; i < size; i++){
			reducer[i] = new Reducer(job, this);
			reducer[i].setName("Reducer" + i);
			reducer[i].start();
		}
	}

	/**
	 * Read files in input directory.
	 *
	 * @param name the directory name
	 */
	public void readFiles(String name) {
		File dir = new File(name);
		File[] files = dir.listFiles();
		BufferedReader br;
		for (File file : files) {
			try {
				if (!file.isDirectory()) {
					br = new BufferedReader(new FileReader(file));
					String line = null;
					while ((line = br.readLine()) != null) {
						synchronized (queue) {
							queue.add(line);
							queue.notify();
						}

					}
				}
			} catch (FileNotFoundException e) {
				System.out.println("Error in opening file ");
			} catch (IOException e) {
				System.out.println("Error in reading from file");
			}

		}

	}

	/**
	 * Check mapper status.
	 *
	 * @param numWorkers the num of  workers
	 */
	private void checkMapperStatus(int numWorkers) {
		boolean waiting = false;
		int count = 0;
		while (true) {
			synchronized (queue) {
				if (queue.isEmpty()) {
					break;
				}

			}
		}

		while (!waiting) {
			count = 0;
			for (Thread th : mapper) {
				if (th.getState() == State.RUNNABLE) {
					break;
				} else if (th.getState() == State.WAITING) {
					count++;
					if (count == numWorkers) {
						waiting = true;
						break;
					}

				}
			}
		}

	}

	/**
	 * Check reducer status.
	 *
	 * @param numWorkers the num workers
	 */
	private void checkReducerStatus(int numWorkers){
		boolean waiting = false;
		int count = 0;
		while (true) {
			synchronized (reduceData) {
				if (reduceData.isEmpty()) {
					break;
				}

			}
		}

		while (!waiting) {
			count = 0;
			for (Thread th : reducer) {
				if (th.getState() == State.RUNNABLE) {
					break;
				} else if (th.getState() == State.WAITING) {
					count++;
					if (count == numWorkers) {
						
						waiting = true;
						break;
					}

				}
			}
		}
		
	}
	
	/**
	 * Creates the dir.
	 *
	 * @param name the dir name
	 */
	public void createDir(String name) {
		File dir = new File(name);
		System.out.println("Deleting dir: " + name);
		if (dir.exists()) {
			File[] files = dir.listFiles();
			for (File file : files){
				System.out.println("Deleting file: " + file.getAbsolutePath());
				file.delete();
				}
			dir.delete();
		}

		dir.mkdir();
	}

	/**
	 * Gets the all workers.
	 *
	 * @return all workers
	 */
	public HashMap<String, String> getAllWorkers() {
		return allWorkers;
	}

	/**
	 * Push.
	 *
	 * @param ipPort the ip port
	 * @param body file content
	 * @return the string
	 */
	private String push(String ipPort, byte[] body){
		StringBuilder request = new StringBuilder();
		String ip = ipPort.split(":")[0];
		int port = Integer.parseInt(ipPort.split(":")[1]);
		try {
			Socket socket = new Socket(ip, port);
			OutputStream out = socket.getOutputStream();
			String workerURL = "http://"+ ipPort+ "/worker/pushdata";
			String postRequest = "POST " + workerURL + " HTTP/1.0\r\n";
			String contentType = "Content-Type:  text/plain\r\n";
			String contentLen = "Content-Length: "+ body.length + "\r\n\r\n";
			out.write(postRequest.getBytes());
			out.write(contentType.getBytes());
			out.write(contentLen.getBytes());
			out.write(body);
			out.flush();
			out.close();
			socket.close();
			System.out.println("PUSHED DATA to " + workerURL);
			
		  } catch (IOException e) {
			  System.out.println("Error while writing to worker's socket");
			  e.printStackTrace();
		  }	  
		return request.toString();
	}
	
	/**
	 * Pushdata.
	 */
	private void pushdata(){
		File dir = new File(spoolOut);
		File[] files = dir.listFiles();
		for (File file : files){
			
			String filename = file.getName();
			if (allWorkers.containsKey(filename)){
				String ipPort = allWorkers.get(filename);
				try{
					FileInputStream fis = new FileInputStream(file);
					BufferedInputStream bis = new BufferedInputStream(fis);
					byte [] body = new byte[(int) file.length()];
					bis.read(body, 0, body.length);
					bis.close();
					push(ipPort, body);
				} catch (IOException e){
					System.out.println("Error while reading from file");
				}
				
			}
		}
	}
	
	/**
	 * Sort map output.
	 */
	private void sortMapOutput(){
		StringBuilder sortedContent = null;
		String sortScript = spoolIn + "/sort.sh";
		File file = new File(sortScript);
		
		if (!file.exists()){
			try {
				file.createNewFile();
				String script = "sort -k 1 -t \\t " + spoolIn + "/*.txt"
						+ " | sort -m";
				
				FileWriter fw = new FileWriter(file);
				fw.write(script);
				fw.flush();
				fw.close();
			} catch (IOException e) {
				System.out.println("Error while creating sort file");
				e.printStackTrace();
			}
			
		}
		
		try {
			file.setExecutable(true);
			Process p = Runtime.getRuntime().exec(sortScript);
			p.waitFor();
			BufferedReader br = new BufferedReader(
		                new InputStreamReader(p.getInputStream()));
			 sortedContent = new StringBuilder();
			 String prevLine = br.readLine();
			 System.out.println("Starting sort...:" + prevLine);
			 sortedContent.append(prevLine +"\n");
			 String line;
			 while ((line = br.readLine()) != null){
				 if (line.equals(prevLine)){
					 System.out.println("Line = prevLine: " + line);
					 sortedContent.append(line + "\n");
				 }
				 else {
					 synchronized(reduceData){
						 reduceData.add(sortedContent.toString());
						 reduceData.notify();
					 }
					 
					 prevLine = line;
					 sortedContent = new StringBuilder();
					 sortedContent.append(prevLine +"\n");
				 }	 
			 }
			 
			 synchronized(reduceData){
				 reduceData.add(sortedContent.toString());
				 reduceData.notify();
			 }
		} catch (IOException e) {
			System.out.println("Error while executing sort.sh");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Sort process interrupted");
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Clear spool in.
	 */
	private void clearSpoolIn(){
		File dir = new File(spoolIn);
		File[] files = dir.listFiles();
		
		for (File file : files){
			file.delete();
		}
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		String html = null;
		String pathInfo = request.getPathInfo();

		if (pathInfo.equals("/runmap")) {
			System.out.println("Got a new job!");

			String mapJob = request.getParameter("job");
			String input = request.getParameter("input");
			int numThreads = Integer.parseInt(request
					.getParameter("numThreads"));
			numWorkers = Integer.parseInt(request
					.getParameter("numWorkers"));
			allWorkers = new HashMap<>();
			for (int i = 1; i <= numWorkers; i++) {
				String key = "worker" + i;
				allWorkers.put(key, request.getParameter(key));
			}

			// get input directory
			input = getServletConfig().getInitParameter("storagedir") + "/"
					+ input;
		

			try {
				Class<?> mapClass = Class.forName(mapJob);
				Job job = (Job) mapClass.newInstance();
				queue = new LinkedList<>();
				spoolOut = getServletConfig().getInitParameter("storagedir")
						+ "spoolOut";

				spoolIn = getServletConfig().getInitParameter("storagedir")
						+ "spoolIn";

				createDir(spoolOut);
				
				createDir(spoolIn);

				// update job
				updateJob(mapJob);
				// update status of the worker
				updateStatus("mapping");

				
				generateMappers(numThreads, job);

				// read all files in input directory
				readFiles(input);

				checkMapperStatus(numThreads);
				stop = true;

				for (Thread th : mapper) {
					if (th.getState() == State.WAITING) {
						th.interrupt();
					}

				}
				for (Thread th : mapper) {
					try {
						th.join();
						System.out.println("Thread joining " + th.getName());
					} catch (InterruptedException e) {
						System.out.println("Error while joining mapper thread");
						e.printStackTrace();
					}
				}
				
				pushdata();
				System.out.println("Done mapping");
				updateStatus("waiting");
				//now push data to corresponding workers
				

			} catch (ClassNotFoundException e) {
				System.out.println("Job class not found");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				System.out.println("Illegal class access exception");
				e.printStackTrace();
			} catch (InstantiationException e) {
				System.out.println("Error while instatiating the Job object");
				e.printStackTrace();
			}

			// generate worker threads

			
		} else if (pathInfo.equals("/runreduce")) {
			System.out.println("GOT REDUCE JOB FROM MASTER");
			String reduceJob = request.getParameter("job");
			int numThreads = Integer.valueOf(request.getParameter("numThreads"));
			output = request.getParameter("output");
			
			output = getServletConfig().getInitParameter("storagedir")
					+ output;
			
			updateStatus("reducing");
			resetKeysWritten();
			createDir(output);
			
			try{
				Class<?> mapClass = Class.forName(reduceJob);
				Job job = (Job) mapClass.newInstance();
				stop = false;
				generateReducers(numThreads, job);
				
				sortMapOutput();
				
				checkReducerStatus(numThreads);
				stop = true;
				

				for (Thread th : reducer) {
					if (th.getState() == State.WAITING) {
						th.interrupt();
					}

				}
				for (Thread th : reducer) {
					try {
						th.join();
						System.out.println("Thread joining " + th.getName());
					} catch (InterruptedException e) {
						System.out.println("Error while joining mapper thread");
						e.printStackTrace();
					}
				}
				
				System.out.println("Job completed");
				
				resetKeysRead();
				heartBeat.interrupt();
				updateStatus("idle");
				
				
			} catch( ClassNotFoundException e){
				System.out.println("Class not found");
			} catch (IllegalAccessException e){
				System.out.println("Illegal access");

			} catch (InstantiationException e){
				System.out.println("Error while instantiating class");
			}
			
			
			
		} else if (pathInfo.equals("/pushdata")){
			
			String ip = request.getRemoteHost();
			String port = String.valueOf(request.getRemotePort());
			String filename = spoolIn + "/" + ip+port+".txt";
			try {
				BufferedReader br = request.getReader();
				File file = new File (filename);
				
				if (file.exists())
					file.delete();
				file.createNewFile();
				FileWriter fw  = new FileWriter(file);
				
				String line ;
				while ((line = br.readLine()) != null){
					fw.write(line + "\n");
				}
				fw.flush();
				fw.close();
				br.close();
			} catch (IOException e) {
				System.out.println("Error while reading the post body");
				e.printStackTrace();
			}
			
		}
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}
}
