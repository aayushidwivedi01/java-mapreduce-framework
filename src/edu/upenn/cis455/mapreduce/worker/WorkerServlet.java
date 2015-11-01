package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.lang.Thread.State;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class WorkerServlet extends HttpServlet {
	private static HashMap<String, String> statusMap;
	private int numWorkers;
	static final long serialVersionUID = 455555002;
	private Mapper[] mapper;
	private Reducer[] reducer;
	private LinkedList<String> queue = new LinkedList<>();
	private static boolean stop = false;
	private String spoolOut;
	private String spoolIn;
	private HashMap<String, String> allWorkers;
	private LinkedList<String> reduceData = new LinkedList<>();

	public void init() {
		statusMap = new HashMap<>();
		statusMapInit();
		String masterPort = getServletConfig().getInitParameter("master");
		HeartBeat heartBeat = new HeartBeat(masterPort, "8080");
		heartBeat.start();
	}

	public void statusMapInit() {
		String port = getServletConfig().getInitParameter("worker");
		statusMap.put("port", port);
		statusMap.put("status", "idle");
		statusMap.put("job", null);
		statusMap.put("keysRead", "0");
		statusMap.put("keysWritten", "0");
	}

	public String getSpoolOut() {
		return spoolOut;
	}

	public String getSpoolIn() {
		return spoolIn;
	}

	public static HashMap<String, String> getStatusMap() {
		return statusMap;
	}

	public void updateStatus(String status) {
		statusMap.put("status", status);
	}

	public void updateJob(String job) {
		statusMap.put("job", job);
	}

	public static boolean getStop() {
		return stop;
	}

	public void generateMappers(int size, WordCount job,
			LinkedList<String> queue, int numWorkers) {
		mapper = new Mapper[size];
		for (int i = 0; i < size; i++) {
			mapper[i] = new Mapper(job, queue, numWorkers, spoolOut);
			mapper[i].setName("Mapper" + i);
			mapper[i].start();
		}
	}
	
	public void generateReducers(int size , WordCount job, String output){
		reducer = new Reducer[size];
		
		for (int i = 0; i < size; i++){
			reducer[i] = new Reducer(job, reduceData, output);
			reducer[i].setName("Reducer" + i);
			reducer[i].start();
		}
	}

	public void readFiles(String name) {
		System.out.println("Dir name: " + name);
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
							System.out.println("Queue: " + queue.toString());
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
					System.out.println("Count: " + count);
					if (count == numWorkers) {
						waiting = true;
						System.out.println("Done waiting:" + count);
						break;
					}

				}
			}
		}

	}

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
					System.out.println("Count: " + count);
					if (count == numWorkers) {
						waiting = true;
						System.out.println("Done waiting:" + count);
						break;
					}

				}
			}
		}
		
	}
	public void createDir(String name) {
		File dir = new File(name);

		if (dir.exists()) {
			System.out.println("Deleting dir: " + name);
			File[] files = dir.listFiles();
			for (File file : files)
				file.delete();
			dir.delete();
		}

		dir.mkdir();
	}

	public HashMap<String, String> getAllWorkers() {
		return allWorkers;
	}

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
	
	private void pushdata(){
		File dir = new File(spoolOut);
		File[] files = dir.listFiles();
		for (File file : files){
			
			String filename = file.getName();
			System.out.println("Files :" + filename);
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
			 BufferedReader br = new BufferedReader(
		                new InputStreamReader(p.getInputStream()));
			 sortedContent = new StringBuilder();
			 String prevLine = br.readLine();
			 sortedContent.append(prevLine +"\n");
			 String line;
			 while ((line = br.readLine()) != null){
				 if (line.equals(prevLine)){
					 sortedContent.append(line + "\n");
				 }
				 else {
					 synchronized(reduceData){
						 reduceData.add(sortedContent.toString());
						 System.out.println("Notifying");
						 reduceData.notify();
					 }
					 
					 prevLine = line;
					 sortedContent = new StringBuilder();
					 sortedContent.append(prevLine +"\n");
				 }
				 
				 
			 }
			 synchronized(reduceData){
				 System.out.println("Reduced data: " + reduceData.toString());
			 }
			 p.waitFor();
			 p.destroy();
		} catch (IOException e) {
			System.out.println("Error while executing sort.sh");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Sort process interrupted");
			e.printStackTrace();
		}
		
	}
	
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
				System.out.println("Worker IP: " + request.getParameter(key));
				allWorkers.put(key, request.getParameter(key));
			}

			// get input directory
			input = getServletConfig().getInitParameter("storagedir") + "/"
					+ input;
		

			try {
				Class<?> mapClass = Class.forName(mapJob);
				WordCount job = (WordCount) mapClass.newInstance();
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

				
				generateMappers(numThreads, job, queue, numWorkers);

				// read all files in input directory
				readFiles(input);

				checkMapperStatus(numWorkers);
				stop = true;

				for (Thread th : mapper) {
					if (th.getState() == State.WAITING) {
						System.out.println("Going to interrupt: "
								+ th.getName());
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
			String reduceJob = request.getParameter("job");
			int numThreads = Integer.valueOf(request.getParameter("numThreads"));
			String output = request.getParameter("output");
			
			output = getServletConfig().getInitParameter("storagedir")
					+ output;
			
			updateStatus("reducing");
			createDir(output);
			
			try{
				Class<?> mapClass = Class.forName(reduceJob);
				WordCount job = (WordCount) mapClass.newInstance();
				System.out.println("running reduce");
				stop = false;
				generateReducers(numThreads, job, output);
				
				sortMapOutput();
				
				checkReducerStatus(numWorkers);
				stop = true;
				

				for (Thread th : reducer) {
					if (th.getState() == State.WAITING) {
						System.out.println("Going to interrupt: "
								+ th.getName());
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
				System.out.println("File written to spoolIn : " + filename);
			} catch (IOException e) {
				System.out.println("Error while reading the post body");
				e.printStackTrace();
			}
			
		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}
}
