package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.HashMap;

public class HeartBeat extends Thread{
	private String masterIpPort;
	private String workerPort;
	private String socketPort;

	public HeartBeat(String ipPort, String workerPort, String socketPort){
		this.masterIpPort = ipPort;
		this.workerPort = workerPort;
		this.socketPort = socketPort;
	}
	
	private int  getPort(){
		int port =  Integer.parseInt(masterIpPort.split(":")[1]);
		return port;
	}
	
	private String getIP(){
		System.out.println(masterIpPort);
		String ip =  masterIpPort.split(":")[0];
		return ip;
	}
	private String buildStatusQuery(HashMap<String, String>statusMap){
		StringBuilder query = new StringBuilder();
		query.append("?port="+workerPort);
		query.append("&status="+statusMap.get("status"));
		query.append("&job="+statusMap.get("job"));
		query.append("&keysRead="+statusMap.get("keysRead"));
		query.append("&keysWritten="+statusMap.get("keysWritten"));		
		
		return query.toString();
	}
	private String getRequest(String query){
		StringBuilder request = new StringBuilder();
		
		request.append("POST "+  masterIpPort+"/workerstatus"+query+ " HTTP/1.0");		
		return request.toString();
		
	}
	public void run(){
		while (true){
			try {
				
				System.out.println("Waking up");
				//TO-DO: update statusMap
				HashMap<String, String>statusMap = WorkerServlet.getStatusMap();
				
				String query = buildStatusQuery(statusMap);
				String request = getRequest(query);
				
				String masterUrl =  "http://"+masterIpPort+"/master/workerstatus";
				Socket socket = new Socket(getIP(), getPort());
				OutputStream out = socket.getOutputStream();
				out.write(("GET " + masterUrl + query+ " HTTP/1.0\r\n\r\n").getBytes());
				out.flush();
				out.close();
				socket.close();
				Thread.sleep(10000);
				
			} catch (InterruptedException e) {
				System.out.println("HeartBeat thread interrrupted");
				e.printStackTrace();
			} catch (IOException e){
				System.out.println("Error opening client socket");
				
			}
		}
		
	}


}
