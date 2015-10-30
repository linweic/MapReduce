package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.log4j.Logger;



import org.apache.log4j.Logger;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  static final Logger logger = Logger.getLogger(MasterServlet.class);
  private Map<String, Map<String,String>> workerMap = new HashMap<String, Map<String,String>>();
  private Map<String,String> jobParams;

  private boolean isWaiting(Map<String,Map<String,String>> workers){
	  for(String key: workers.keySet()){
		  Map<String,String> statusParams = workers.get(key);
		  if(statusParams.get("status").equals("waiting")==false) return false;
	  }
	  return true;
  }
  private void postToReduce(InetAddress address, String port, Map<String, String> workerParams) throws IOException{
	  HttpClient client = new HttpClient(address, Integer.valueOf(port), workerParams);
	  client.setRequestMethod("post");
	  try {
		client.setRequestURL(workerParams.get("name"),"/runreduce");
		} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		}
	  client.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("job=").append(jobParams.get("job")).append("&");
	  sb.append("output=").append(jobParams.get("outputDirectory")).append("&");
	  sb.append("numThreads=").append(jobParams.get("reduceThreadsNumber"));
	  int length = sb.length();
	  
	  client.setRequestHeader("Content-Length", String.valueOf(length));
	  client.setRequestBody(sb);
	  client.requestFlush();
	  BufferedReader br = client.getInputStreamReader();
	  System.out.println("[debug]first line from /runreduce response: "+ br.readLine());
	  client.closeClient();
	  
  }
  @SuppressWarnings("unchecked")
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String pathInfo = request.getPathInfo();
	  System.out.println("[debug] submitted form's path info is: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/status")){
		  jobParams = request.getParameterMap();
		  
		  System.out.println("-----Job parameters-----");
		  for(String jobkey: jobParams.keySet()){
			  System.out.println(jobkey+": "+jobParams.get(jobkey));
		  }
		  System.out.println("----------");
		  
		  for(String workerKey: workerMap.keySet()){
			  Map<String, String> paramMap = workerMap.get(workerKey);
			  String status = paramMap.get("status");
			  System.out.println("[debug] worker "+ workerKey+"'s status is "+status);
			  if(status.equals("idle")){
				  String[] strings = workerKey.split(":");
				  System.out.println("[debug] worker's IP address is "+strings[0]);
				  System.out.println("[debug] worker's port number is "+strings[1]);
				  InetAddress ipAddress = InetAddress.getByName(strings[0]);
				  HttpClient client = new HttpClient(ipAddress, Integer.valueOf(strings[1]),paramMap);
				  client.setRequestMethod("post");
				  try {
					  client.setRequestURL(paramMap.get("name"),"/runmap");
				  } catch (Exception e) {
					  // TODO Auto-generated catch block
					  e.printStackTrace();
				  }
				  client.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
				  
				  StringBuffer sb = new StringBuffer();
				  sb.append("job=").append(jobParams.get("job")).append("&");
				  sb.append("input=").append(jobParams.get("inputDirectory")).append("&");
				  sb.append("numThreads=").append(jobParams.get("mapThreadsNumber")).append("&");
				  sb.append("numWorkers=").append(workerMap.keySet().size());
				  int i = 1;
				  for(String key: workerMap.keySet()){
					  sb.append("&").append("worker").append(i).append("=").append(key);
					  i++;
				  }
				  int length = sb.length();				  
				  
				  client.setRequestHeader("Content-Length", String.valueOf(length));
				  client.setRequestBody(sb);
				  client.requestFlush();
				  BufferedReader br = client.getInputStreamReader();
				  System.out.println("[debug]first line response from /runmap: "+ br.readLine());
				  client.closeClient();
			  }
		  }
	  }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    String IP;
    String port;
    String hostname;
    String comb;
    String status;
    String job;
    String keysRead;
    String keysWritten;
    String receivedTime;
    
    String pathInfo = request.getPathInfo();
    String contextPath = request.getContextPath();
    System.out.println("[debug] path info: "+pathInfo);
    System.out.println("[debug] context path(webapp): "+ contextPath);
    if(pathInfo.equalsIgnoreCase("/workerstatus")){
    	IP = request.getRemoteAddr();
    	port = request.getParameter("port");
    	comb = IP + ":" + port;
    	hostname = request.getRemoteHost();
    	@SuppressWarnings("unchecked")
		Map<String,String> paramMap = request.getParameterMap();
    	
    	System.out.println("-----worker status map-----");
    	for(String key : paramMap.keySet()){
    		System.out.println(key+": "+paramMap.get(key));
    	}
    	System.out.println("-----------");
    	
    	Date date = new Date();
    	long time = date.getTime();
    	receivedTime = String.valueOf(time);
    	paramMap.put("lastSubmit", receivedTime);
    	paramMap.put("hostname", hostname);
    	workerMap.put(comb, paramMap);
    	if(isWaiting(workerMap) == true){
    		for(String key: workerMap.keySet()){
    			String[] strings = key.split(":");
    			postToReduce(InetAddress.getByName(strings[0]),strings[1], workerMap.get(key));
    		}
    	};
    }
    else if(pathInfo.equalsIgnoreCase("/status")){
    	response.setContentType("text/html");
        PrintWriter out = response.getWriter();
    	out.println("<html><head><title>Status</title></head>");
    	out.println("<body><h1>Active Workers Status Information</h1><br>");
    	out.println("<table border=\"1\" style=\"width:100%\">");
    	out.println("<tr><th>\"IP:port\"</th><th>\"status\"</th><th>\"job\"</th>"
    			+ "<th>\"keys read\"</th><th>\"keys written\"</th></tr>");
   	
    	Date date = new Date();
    	long current = date.getTime();
    	if(workerMap.keySet().isEmpty() == true) System.out.println("[info]no workers submitted their status yet.");
    	else{
    		for(String key:workerMap.keySet()){
    			Map<String,String> paramMap = workerMap.get(key);
    			String lastSubmittedTime = paramMap.get("lastSubmit");
    			if(current - Long.valueOf(lastSubmittedTime)<30000){
    				System.out.println("[info]"+key+" worker is active");
    				status = paramMap.get("status");
    				job = paramMap.get("job");
    				keysRead = paramMap.get("keysRead");
    				keysWritten = paramMap.get("keysWritten");
    				out.println("<tr><td>"+key+"</td>");
    				out.println("<td>"+status+"</td>");
    				out.println("<td>"+job+"</td>");
    				out.println("<td>"+keysRead+"</td>");
    				out.println("<td>"+keysWritten+"</td></tr>");
    			}
    			else{
    				System.out.println("[debug]this worker "+key+" is not activem, remove it from workerMap");
    				workerMap.remove(key);
    			}
    		}
    	}
    	out.println("</table><br>");
    	
    	StringBuffer action = new StringBuffer(contextPath);
    	action.append(pathInfo);
    	System.out.println("[debug]/status form post route is: "+ action);
    	out.println("<form action="+action+" method=\"post\">");
    	out.println("class name of job:<br>");
    	out.println("<input type=\"text\" name=\"job\"><br>");
    	out.println("input directory(relative to storage directory):<br>");
    	out.println("<input type=\"text\" name=\"inputDirectory\"><br>");
    	out.println("output directory(relative to storage directory):<br>");
    	out.println("<input type=\"text\" name=\"outputDirectory\"><br>");
    	out.println("number of map threads to run on each worker:<br>");
    	out.println("<input type=\"text\" name=\"mapThreadsNumber\"><br>");
    	out.println("number of reduce threads to run on each worker:<br>");
    	out.println("<input type=\"text\" name=\"reduceThreadsNumber\"><br>");
    	out.println("<input type=\"submit\" value=\"Submit\"></form>");
    	
    	out.println("</body></html>");
    }
  }
}
  
