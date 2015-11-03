package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class HttpClient {
	static final Logger logger = Logger.getLogger(HttpClient.class);
	private Socket clientSocket;
	private PrintWriter out;
	private BufferedReader in;
	private String method;
	private String hostname;
	private int port;
	private Map<String,String> resHeaderMap;
	private Map<String,String> paramMap;
	
	public HttpClient(InetAddress address, int port) throws IOException{
		clientSocket = new Socket(address, port);
		resHeaderMap = new HashMap<String, String>();
		out = new PrintWriter(clientSocket.getOutputStream(),false);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	}
	public HttpClient(InetAddress address, int port, String name) throws IOException{
		clientSocket = new Socket(address, port);
		resHeaderMap = new HashMap<String, String>();
		hostname = name;
		this.port = port;
		out = new PrintWriter(clientSocket.getOutputStream(),false);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	}
	public void setRequestMethod(String m){
		method = m;
	}
	public void setRequestURL(String worker, String url) throws Exception{
		if(method == null) throw new Exception("request method not set");
		else{
			StringBuffer sb = new StringBuffer();
			if(worker.equals("master")){
				sb.append(method.toUpperCase()).append(" /").append(url).append(" HTTP/1.1");
			}
			else{
				sb.append(method.toUpperCase()).append(" /").append(worker).append("/").append(url).append(" HTTP/1.1");
			}
			//System.out.println("[debug]first request line: "+sb);
			out.println(sb);
			sb = new StringBuffer("Host: ");
			sb.append(hostname).append(":").append(port);
			//System.out.println("[debug]host header info => "+ sb);
			out.println(sb);
		}
	}
	public void setRequestHeader(String headername, String value){
		resHeaderMap.put(headername, value);
		StringBuffer sb = new StringBuffer(headername);
		sb.append(": ").append(value);
		//System.out.println("[debug]header line => "+ sb);
		out.println(sb);
	}
	public void setRequestBody(StringBuffer sb){
		out.println("\r\n");
		out.println(sb);
	}
	public void sendNewLine(){
		out.println("\r\n");
	}
	public void requestFlush(){
		System.out.println("start flushing...");
		out.println("\r\n");
		out.flush();
	}
	public BufferedReader getInputStreamReader(){
		return in;
	}
	public void closeClient() throws IOException{
		out.close();
		in.close();
		clientSocket.close();
	}
	
	public static void main(String[] args) throws Exception{
		/*
		InetAddress addr = InetAddress.getByName("127.0.0.1");
		String hostname = addr.getHostName();
		HttpClient client = new HttpClient(addr, 3000, hostname);
		client.setRequestMethod("GET");
		StringBuffer queryString = new StringBuffer("?");
		queryString.append("status=").append("idle").append("&");
		queryString.append("port=").append("8080").append("&");
		queryString.append("job=").append("unknown").append("&");
		queryString.append("keysRead=").append("0").append("&");
		queryString.append("keysWritten=").append("0");
		//queryString.append("name=").append(workerName);
		StringBuffer url = new StringBuffer("workerstatus");
		url.append(queryString);
		System.out.println("path info of this request is:");
		System.out.println(url);
		client.setRequestURL("master", url.toString());
		client.sendNewLine();
		client.requestFlush();
		BufferedReader br = client.getInputStreamReader();
		//System.out.println("[debug]first line response from /workerstatus: "+ br.readLine());
		String line;
		System.out.println("------");
		while((line = br.readLine())!=null){
			System.out.println(line);
		}
		client.closeClient();		  
		//System.out.println("---worker status submitted successfully");
		*/
		
		InetAddress ipAddress = InetAddress.getByName("127.0.0.1");
		String hostname = ipAddress.getHostName();
		HttpClient client = new HttpClient(ipAddress, 8080,hostname);
		client.setRequestMethod("post");
		try {
			client.setRequestURL("worker","runmap");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
		  
		StringBuffer sb = new StringBuffer();
		sb.append("job=").append("edu.upenn.cis455.mapreduce.job.WordCount").append("&");
		sb.append("input=").append("input").append("&");
		sb.append("numThreads=").append("2").append("&");
		sb.append("numWorkers=").append("1").append("&").append("dummy=dummy");
		
		//sb.append("&").append("worker1").append(i).append("=").append(key);
		int length = sb.length();				  
		System.out.println("[debug] the body msg of this /runmap request is:");
		System.out.println(sb);
		client.setRequestHeader("Content-Length", String.valueOf(length));
		client.sendNewLine();
		client.setRequestBody(sb);
		client.sendNewLine();
		client.requestFlush();
		BufferedReader br = client.getInputStreamReader();
		System.out.println("[debug]first line response from /runmap: "+ br.readLine());
		String s;
		while((s = br.readLine())!=null){
			System.out.println(s);
		}
		client.closeClient();
	}
	
}
