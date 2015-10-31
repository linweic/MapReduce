package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.MapContextImpl;
import edu.upenn.cis455.mapreduce.Job;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  static final Logger logger = Logger.getLogger(WorkerServlet.class);
  /*
  public synchronized void readFileToMemo(BlockingQueue<String> queue, BufferedReader br) throws IOException, InterruptedException{
		String line;
		while((line = br.readLine())!=null){
			queue.put(line);
			logger.debug(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
		}
  }
  */
  private void deleteDir(File dir){
	  if(dir.isDirectory()){
		  File[] files = dir.listFiles();
		  if(files != null && files.length>0){
			  for(File file: files){
				  deleteDir(file);
			  }
		  }
		  else dir.delete();
	  }
	  else dir.delete();
  }
  private File makeSpoolOutDir(String storageDir) throws IOException{
	  String spoolOut = storageDir.concat("spool-out");
	  logger.debug("spool out file path is "+ spoolOut);
	  File outDir = new File(spoolOut);
	  if(outDir.exists()) deleteDir(outDir);
	  outDir.mkdir();
	  return outDir;
  }
  private File[] createWorkerFiles(String storageDir, int numWorkers) throws IOException{
	  File[] files = new File[numWorkers];
	  for(int i = 0; i<numWorkers; i++){
		  String path = storageDir.concat("spool-out/worker").concat(String.valueOf(i+1));
		  File f = new File(path);
		  f.createNewFile();
		  files[i] = f;
	  }
	  return files;
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String comb = getServletConfig().getInitParameter("master");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  logger.debug("-----worker debug section-----");
	  logger.debug("[debug]ip:port of this master is =>"+comb);
	  logger.debug("[debug]storage dir is => "+storageDir);
	  
	  String pathInfo = request.getPathInfo();
	  String remoteAddr = request.getRemoteAddr();
	  int remotePort = request.getRemotePort();
	  String remoteComb = remoteAddr.concat(":").concat(String.valueOf(remotePort));
	  logger.debug("[debug] remote ip:port is=>"+remoteComb);
	  logger.debug("[debug] worker path info: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/runmap")){
		  if(remoteComb.equals(comb)){
			  logger.debug("worker receives post request from master");
			  String jobName = request.getParameter("job");
			  String input = request.getParameter("input");
			  String num = request.getParameter("numThreads");
			  String stringNumWorkers = request.getParameter("numWorkers");
			  int numThreads = Integer.valueOf(num);
			  int numWorkers = Integer.valueOf(stringNumWorkers);
			  
			  File spoolOutDir = makeSpoolOutDir(storageDir);
			  logger.info("spoolout created");
			  File[] spoolOutFiles = createWorkerFiles(storageDir, numWorkers);
			  logger.info("files for every worker in spoolout created");;
			  
			  Job jobInstance = null;		  
			  try {
				  Class<?> jobClass = Class.forName(jobName);
				  jobInstance = (Job)jobClass.newInstance();
			  } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				  // TODO Auto-generated catch block
				  e.printStackTrace();
			  }
			  Context contextInstance = new MapContextImpl(spoolOutFiles);
			  logger.info("context instance created");
			  
			  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
			  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
			  for(int i = 0; i<numThreads-1; i++){
					service.submit(new WorkerThread(queue, jobInstance, contextInstance));
			  }
			  String inputDir = storageDir.concat(input);
			  logger.debug("[debug] input directory full path is"+inputDir);
			  File folder = new File(inputDir);
			  File[] files = folder.listFiles();
			  int length = files.length;
			  logger.debug("reading files from disks to memory");
			  for(int j = 0; j<length;j++){
					BufferedReader br = new BufferedReader(new FileReader(files[j]));
					try {
						service.submit(new FileTask(queue,br)).get();
					} catch (InterruptedException | ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			  }
			  service.shutdown();
		  }
		  else{
			  logger.debug("worker receives post request from other workers");
		  }
	  }
  }
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String comb = getServletConfig().getInitParameter("master");
	  logger.debug("-----worker debug section-----");
	  logger.debug("[debug]ip:port of this master is =>"+comb);
	  String[] strings = comb.split(":");
	  String masterIP = strings[0];
	  String masterPort = strings[1];
	  
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  
  }
}
  
