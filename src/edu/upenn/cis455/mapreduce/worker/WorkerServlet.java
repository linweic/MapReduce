package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.ContextImpl;
import edu.upenn.cis455.mapreduce.Job;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  public synchronized void readFileToMemo(BlockingQueue<String> queue, BufferedReader br) throws IOException, InterruptedException{
		String line;
		while((line = br.readLine())!=null){
			queue.put(line);
			System.out.println(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
		}
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String comb = getServletConfig().getInitParameter("master");
	  System.out.println("-----worker debug section-----");
	  System.out.println("[debug]ip:port of this master is =>"+comb);
	  
	  String pathInfo = request.getPathInfo();
	  String remoteAddr = request.getRemoteAddr();
	  int remotePort = request.getRemotePort();
	  String remoteComb = remoteAddr.concat(":").concat(String.valueOf(remotePort));
	  System.out.println("[debug] remote ip:port is=>"+remoteComb);
	  System.out.println("[debug] worker path info: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/runmap")){
		  if(remoteComb.equals(comb)){
			  System.out.println("worker receives post request from master");
			  String jobName = request.getParameter("job");
			  String input = request.getParameter("input");
			  String num = request.getParameter("numThreads");
			  int numThreads = Integer.valueOf(num);
			  
			  Job jobInstance = null;		  
			  try {
				  Class<?> jobClass = Class.forName(jobName);
				  jobInstance = (Job)jobClass.newInstance();
			  } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				  // TODO Auto-generated catch block
				  e.printStackTrace();
			  }
			  Context contextInstance = null;
			  try {
				  Class<?> contextClass = Class.forName("edu.upenn.cis455.mapreduce.ContextImpl");
				  contextInstance = (Context)contextClass.newInstance();
			  } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				  // TODO Auto-generated catch block
				  e.printStackTrace();
			  }
			  
			  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
			  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
			  for(int i = 0; i<numThreads; i++){
					service.submit(new WorkerThread(queue, jobInstance, contextInstance));
			  }
			  System.out.println("[debug] input directory is"+input);
			  File folder = new File(input);
			  File[] files = folder.listFiles();
			  int length = files.length;
			  for(int j = 0; j<length;j++){
					BufferedReader br = new BufferedReader(new FileReader(files[j]));
					try {
						readFileToMemo(queue, br);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			  }
			  service.shutdownNow();
			  try {
				service.awaitTermination(365,TimeUnit.DAYS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  else{
			  System.out.println("worker receives post request from other workers");
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
  
