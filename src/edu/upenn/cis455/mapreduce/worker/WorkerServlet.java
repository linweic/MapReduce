package edu.upenn.cis455.mapreduce.worker;

import java.io.*;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String pathInfo = request.getPathInfo();
	  System.out.println("[debug] worker path info: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/runmap")){
		  String jobName = request.getParameter("job");
		  String input = request.getParameter("input");
		  String numThreads = request.getParameter("numThreads");
		  
		  for(int i = 0; i<numThread; i++){
			  
		  }
		  try {
			Class<?> jobClass = Class.forName(jobName);
			Job jobInstance = (Job)jobClass.newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
  }
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String comb = getServletConfig().getInitParameter("master");
	  System.out.println("-----worker debug section-----");
	  System.out.println("[debug]ip:port of this master is =>"+comb);
	  String[] strings = comb.split(":");
	  String masterIP = strings[0];
	  String masterPort = strings[1];
	  
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  
  }
}
  
