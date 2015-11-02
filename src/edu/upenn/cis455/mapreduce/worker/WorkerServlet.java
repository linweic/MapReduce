package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.InetAddress;
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
import edu.upenn.cis455.mapreduce.ReduceContextImpl;
import edu.upenn.cis455.mapreduce.master.HttpClient;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  static final Logger logger = Logger.getLogger(WorkerServlet.class);
  public static String status;
  public static int keysRead=0;
  public static int keysWritten = 0;
  
  public synchronized void readFileToMemo(BlockingQueue<String> queue, BufferedReader br) throws IOException, InterruptedException{
		String line;
		while((line = br.readLine())!=null){
			queue.put(line);
			logger.debug(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
		}
  }
  
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
  private File makeOutputDir(String storageDir,String dirName) throws IOException{
	  String dirPath = storageDir.concat(dirName);
	  logger.debug(dirName+"'s file path is "+ dirPath);
	  File outDir = new File(dirPath);
	  if(outDir.exists()) deleteDir(outDir);
	  outDir.mkdir();
	  return outDir;
  }
  private void makeSpoolInDir(String storageDir){
	  String spoolIn = storageDir.concat("spool-in");
	  logger.debug("spool-in file path is "+ spoolIn);
	  File inDir = new File(spoolIn);
	  if(inDir.exists()) deleteDir(inDir);
	  inDir.mkdir();
	  logger.debug("spool-in directory has been created.");
	  String filePath = spoolIn.concat("/intermediate");
	  File f = new File(filePath);
	  try {
		f.createNewFile();
		logger.debug("intermediate file in spool-in has been created.");
	  } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
  }
  private File makeFile(String storageDir, String parentDir, String filename){
	  StringBuffer sb = new StringBuffer(storageDir);
	  sb.append(parentDir).append(parentDir).append("/").append(filename);
	  logger.debug(filename+" path is: "+ sb);
	  File file = new File(sb.toString());
	  try {
		file.createNewFile();
	  } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
	  return file;
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
  private void reportStatus(String masterParam, String job) throws Exception{
	  logger.debug("reporting status to master");
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  String workerName = getServletConfig().getInitParameter("workername");
	  logger.debug("workername is "+ workerName);
	  String[] values = masterParam.trim().split(":");
	  InetAddress masterIP = InetAddress.getByName(values[0]);
	  String hostname = masterIP.getHostName();
	  HttpClient statusClient = new HttpClient(masterIP,Integer.valueOf(values[1]),hostname);
	  statusClient.setRequestMethod("GET");
	  StringBuffer queryString = new StringBuffer("?");
	  queryString.append("status=").append(status).append("&");
	  queryString.append("port=").append(workerPort).append("&");
	  queryString.append("job=").append(job).append("&");
	  queryString.append("keysRead=").append(keysRead).append("&");
	  queryString.append("keysWritten=").append(keysWritten).append("&");
	  queryString.append("name=").append(workerName);
	  StringBuffer url = new StringBuffer("workerstatus");
	  url.append(queryString);
	  logger.debug("path info of this request is:");
	  logger.debug(url);
	  statusClient.setRequestURL(workerName, url.toString());
	  statusClient.requestFlush();
	  BufferedReader br = statusClient.getInputStreamReader();
	  logger.debug("[debug]first line response from /workerstatus: "+ br.readLine());
	  statusClient.closeClient();		  
	  logger.debug("---worker status submitted successfully");
  }
  private void sortFile(File workDir){
	  try {
		  ProcessBuilder pb = new ProcessBuilder("sort","-o","./intermediate","./intermediate");
		  pb.directory(workDir);
		  Process process;
		  process = pb.start();
		  BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		  String line;
		  logger.debug("---sorted intermediate file---");
		  while((line = br.readLine())!= null){
			  logger.debug(line);
		  }
		  logger.debug("----------");
	  } catch (IOException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  }
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String masterComb = getServletConfig().getInitParameter("master");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  logger.debug("-----worker debug section-----");
	  logger.debug("[debug]ip:port of this master is =>"+masterComb);
	  logger.debug("[debug]storage dir is => "+storageDir);
	  
	  String pathInfo = request.getPathInfo();
	  String remoteAddr = request.getRemoteAddr();
	  int remotePort = request.getRemotePort();
	  String remoteComb = remoteAddr.concat(":").concat(String.valueOf(remotePort));
	  logger.debug("[debug] remote ip:port is=>"+remoteComb);
	  logger.debug("[debug] worker path info: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/runmap")){
		  logger.debug("worker receives post request from master, worker starts doing mapping...");
		  keysRead = 0;
		  keysWritten = 0;
		  status = "mapping";
		  String jobName = request.getParameter("job");
		  String input = request.getParameter("input");
		  String num = request.getParameter("numThreads");
		  String stringNumWorkers = request.getParameter("numWorkers");
		  int numThreads = Integer.valueOf(num);
		  int numWorkers = Integer.valueOf(stringNumWorkers);
			 
		  File spoolOutDir = makeOutputDir(storageDir,"spool-out");
		  logger.info("spoolout created");
		  File[] spoolOutFiles = createWorkerFiles(storageDir, numWorkers);
		  logger.info("files for every worker in spoolout created");
		  makeSpoolInDir(storageDir);
		  Job jobInstance = null;		  
		  try {
			  Class<?> jobClass = Class.forName(jobName);
			  jobInstance = (Job)jobClass.newInstance();
		  } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  Context contextInstance = new MapContextImpl(spoolOutFiles);
		  logger.info("map context instance created");
			  
		  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
		  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
		  for(int i = 0; i<numThreads-1; i++){
			  service.submit(new MapThread(queue, jobInstance, contextInstance));
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
		  logger.debug("all threads have finished hashing and writing to spool-out files.");
		  logger.debug("[keysRead] "+keysRead+" keys have been read by last map");
		  logger.debug("[keysWritten] "+ keysWritten+" keys have been written by last map");
		  logger.debug("now posting contents of files to /pushdata of other workers");
		  for(int i = 0; i< numWorkers; i++){
			  StringBuffer workerName = new StringBuffer("worker");
			  workerName.append(i+1);
			  String IPAndPort = request.getParameter(workerName.toString());
			  String[] values = IPAndPort.trim().split(":");
			  InetAddress ip = InetAddress.getByName(values[0]);
			  String hostname = ip.getHostName();
			  HttpClient client = new HttpClient(ip, Integer.valueOf(values[1]),hostname);
			  client.setRequestMethod("POST");
			  try {
				client.setRequestURL(workerName.toString(), "pushdata");
			  } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			  }
			  StringBuffer filePath = new StringBuffer(storageDir);
			  filePath.append("spool-out/").append(workerName);
			  logger.debug("file path of "+workerName+"is: "+filePath);
			  File mapFile = new File(filePath.toString());
			  BufferedReader mapFileReader = new BufferedReader(new FileReader(mapFile));
			  String line;
			  StringBuffer body = new StringBuffer();
			  while((line = mapFileReader.readLine())!=null){
				  body.append(line).append("\r\n");
			  }
			  mapFileReader.close();
			  int bodyLength = body.length();
			  client.setRequestHeader("Content-Type","text/plain");
			  client.setRequestHeader("Content-Length", String.valueOf(bodyLength));
			  client.setRequestBody(body);
			  client.requestFlush();
			  BufferedReader br = client.getInputStreamReader();
			  logger.debug("[debug]first line response from /pushdata: "+ br.readLine());
			  client.closeClient();			  
		  }
		  logger.debug("-------map results posted to /pushdata------");
		  logger.debug("worker status changes to waiting");
		  status = "waiting";
		  try {
			reportStatus(masterComb,jobName);
		  } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
		  logger.debug("status updated...waiting");
	  }
	  else if(pathInfo.equalsIgnoreCase("/pushdata")){
		  logger.debug("receieves post request from peer workers");
		  logger.debug("start writing body messages into spool-in directory...");
		  BufferedReader contentReader = request.getReader();
		  File intermediate = new File(storageDir+"spool-in/intermediate");
		  PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(intermediate,true)));
		  String line;
		  logger.debug("-----content data-----");
		  while((line = contentReader.readLine())!=null){
			  logger.debug(line);
			  pw.println(line);
		  }
		  logger.debug("-----data in content have been written to spool-in file");
		  pw.close();
	  }
	  else if(pathInfo.equalsIgnoreCase("/runreduce")){
		  logger.debug("worker starts doing reducing...");
		  keysRead = 0;
		  keysWritten = 0;
		  status = "reducing";
		  String jobName = request.getParameter("job");
		  String output = request.getParameter("output");
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
		  //File outputDir = new File(output);
		  makeOutputDir(storageDir,output);
		  logger.debug("output directory is created.");
		  File outputData = makeFile(storageDir, output, "outputdata");
		  logger.debug("outputdata file is created.");
		  File spoolIn = new File(storageDir+"spool-in");
		  File intermediate = new File(storageDir+"spool-in/intermediate");
		  logger.debug("start sorting the intermediate file");
		  sortFile(spoolIn);
		  Context contextInstance = new ReduceContextImpl(outputData);
		  logger.info("reduce context instance created");
		  
		  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
		  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
		  for(int i = 0; i<numThreads; i++){
			  service.submit(new ReduceThread(queue, jobInstance, contextInstance));
		  }
		  logger.debug("reading files from disks to memory");
		  BufferedReader br = new BufferedReader(new FileReader(intermediate));
		  try {
			  readFileToMemo(queue,br);
		  } catch (InterruptedException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  service.shutdown();
		  logger.debug("reducing finished");
		  status = "idle";
		  try {
			reportStatus(masterComb, jobName);
		  } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
		  logger.debug("status updated...idle");
	  }
  }
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String masterComb = getServletConfig().getInitParameter("master");
	  logger.debug("-----worker debug section-----");
	  logger.debug("[debug]ip:port of this master is =>"+masterComb);
	  String[] strings = masterComb.split(":");
	  String masterIP = strings[0];
	  String masterPort = strings[1];
	  
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  
  }
}
  
