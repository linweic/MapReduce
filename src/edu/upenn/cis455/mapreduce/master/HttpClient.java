package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class HttpClient {
	static final Logger logger = Logger.getLogger(HttpClient.class);
	private Socket clientSocket;
	private PrintWriter out;
	private BufferedReader in;
	private String method;
	private Map<String,String> resHeaderMap;
	private Map<String,String> paramMap;
	
	public HttpClient(InetAddress address, int port) throws IOException{
		clientSocket = new Socket(address, port);
		resHeaderMap = new HashMap<String, String>();
		out = new PrintWriter(clientSocket.getOutputStream(),false);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	}
	public HttpClient(InetAddress address, int port, Map<String,String>paramMap) throws IOException{
		clientSocket = new Socket(address, port);
		resHeaderMap = new HashMap<String, String>();
		this.paramMap = paramMap; 
		out = new PrintWriter(clientSocket.getOutputStream(),false);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		resHeaderMap.put("hostname", paramMap.get("hostname"));
	}
	public void setRequestMethod(String m){
		method = m;
	}
	public void setRequestURL(String worker, String url) throws Exception{
		if(method == null) throw new Exception("request method not set");
		else{
			StringBuffer sb = new StringBuffer();
			sb.append(method.toUpperCase()).append(" /").append(worker).append("/").append(url).append(" HTTP/1.1");
			System.out.println("[debug]first request line: "+sb);
			out.println(sb);
			sb = new StringBuffer("Host:");
			sb.append(resHeaderMap.get("hostname"));
			System.out.println("[debug]host header info => "+ sb);
			out.println(sb);
		}
	}
	public void setRequestHeader(String headername, String value){
		resHeaderMap.put(headername, value);
		StringBuffer sb = new StringBuffer(headername);
		sb.append(": ").append(value);
		System.out.println("[debug]header line => "+ sb);
		out.println(sb);
	}
	public void setRequestBody(StringBuffer sb){
		out.println("\r\n");
		out.println(sb);
	}
	public void requestFlush(){
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
}
