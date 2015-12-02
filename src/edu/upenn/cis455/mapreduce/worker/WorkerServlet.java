package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
  public static int keysRead;
  public static int keysWritten;
  public static String jobName;
  
  public synchronized void readFileToMemo(BlockingQueue<String> queue, BufferedReader br) throws IOException, InterruptedException{
		String line;
		while((line = br.readLine())!=null){
			queue.put(line);
			System.out.println(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
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
	  System.out.println(dirName+"'s file path is "+ dirPath);
	  File outDir = new File(dirPath);
	  if(outDir.exists()) deleteDir(outDir);
	  outDir.mkdir();
	  return outDir;
  }
  private void makeSpoolInDir(String storageDir){
	  String spoolIn = storageDir.concat("spool-in");
	  System.out.println("spool-in file path is "+ spoolIn);
	  File inDir = new File(spoolIn);
	  if(inDir.exists()){
		  deleteDir(inDir);
		  System.out.println("deleted previous spool-in directory");
	  }
	  inDir.mkdir();
	  System.out.println("spool-in directory has been created.");
	  String filePath = spoolIn.concat("/intermediate");
	  File f = new File(filePath);
	  try {
		f.createNewFile();
		System.out.println("intermediate file in spool-in has been created.");
	  } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
  }
  private File makeFile(String storageDir, String parentDir, String filename){
	  StringBuffer sb = new StringBuffer(storageDir);
	  sb.append(parentDir).append("/").append(filename);
	  System.out.println(filename+" path is: "+ sb);
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
  private void reportStatus() throws Exception{
	  System.out.println("reporting status to master");
	  String masterParam = getServletConfig().getInitParameter("master");
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  String[] values = masterParam.trim().split(":");
	  //System.out.println("master ip address is: "+values[0]);
	  InetAddress masterIP = InetAddress.getByName(values[0]);
	  String hostname = masterIP.getHostName();
	  //System.out.println("master hostname is: "+hostname);
	  HttpClient statusClient = new HttpClient(masterIP,Integer.valueOf(values[1]),hostname);
	  statusClient.setRequestMethod("GET");
	  StringBuffer queryString = new StringBuffer("?");
	  queryString.append("status=").append(status).append("&");
	  queryString.append("port=").append(workerPort).append("&");
	  queryString.append("job=").append(jobName).append("&");
	  queryString.append("keysRead=").append(keysRead).append("&");
	  queryString.append("keysWritten=").append(keysWritten);
	  StringBuffer url = new StringBuffer("workerstatus");
	  url.append(queryString);
	  //System.out.println("path info of this request is:");
	  //System.out.println(url);
	  statusClient.setRequestURL("master", url.toString());
	  statusClient.sendNewLine();
	  statusClient.requestFlush();
	  BufferedReader br = statusClient.getInputStreamReader();
	  //System.out.println("[debug]first line response from /workerstatus: "+ br.readLine());
	  statusClient.closeClient();		  
	  System.out.println("---worker status submitted successfully");
  }
  private void sortFile(File workDir){
	  try {
		  ProcessBuilder pb = new ProcessBuilder("sort","-o","./intermediate","./intermediate");
		  pb.directory(workDir);
		  Process process;
		  process = pb.start();
		  BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		  String line;
		  System.out.println("---sorted intermediate file---");
		  while((line = br.readLine())!= null){
			  System.out.println(line);
		  }
		  System.out.println("----------");
	  } catch (IOException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  }
  }
  private class StatusTask extends TimerTask{
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			reportStatus();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	  
  }
  public void init(){
	  System.out.println("initing worker servlet");
	  status = "idle";
	  keysRead = 0;
	  keysWritten = 0;
	  jobName = "unknown";
	  Timer timer = new Timer();
	  timer.schedule(new StatusTask(), 0, 30000);
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  makeSpoolInDir(storageDir);
  }
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException{
	  String masterComb = getServletConfig().getInitParameter("master");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  //System.out.println("-----worker debug section-----");
	  //System.out.println("[debug]ip:port of this master is =>"+masterComb);
	  //System.out.println("[debug]storage dir is => "+storageDir);
	  
	  String pathInfo = request.getPathInfo();
	  String remoteAddr = request.getRemoteAddr();
	  int remotePort = request.getRemotePort();
	  String remoteComb = remoteAddr.concat(":").concat(String.valueOf(remotePort));
	  //System.out.println("[debug] remote ip:port is=>"+remoteComb);
	  System.out.println("[debug] worker path info: "+ pathInfo);
	  if(pathInfo.equalsIgnoreCase("/worker/runmap")){
		  System.out.println("worker receives post request from master, worker starts doing mapping...");
		  keysRead = 0;
		  keysWritten = 0;
		  status = "mapping";
		  @SuppressWarnings("unchecked")
		  Map<String,String[]> jobParams = request.getParameterMap();
		  System.out.println(jobParams.keySet());
		  for(String s: jobParams.keySet()) {
			  System.out.println(s+"\t"+jobParams.get(s)[0]);
			  if(s.contains("job")) jobName = jobParams.get(s)[0];
		  }
		  //String job = jobParams.get("\r\njob")[0];
		  System.out.println("job name is: "+ jobName);
		  String input = jobParams.get("input")[0];
		  System.out.println("input path is: "+ input);
		  String num = jobParams.get("numThreads")[0];
		  String stringNumWorkers = jobParams.get("numWorkers")[0];
		  int numThreads = Integer.valueOf(num);
		  int numWorkers = Integer.valueOf(stringNumWorkers);
		  System.out.println("number of workers is"+ numWorkers); 
		  File spoolOutDir = makeOutputDir(storageDir,"spool-out");
		  System.out.println("spoolout created");
		  File[] spoolOutFiles = createWorkerFiles(storageDir, numWorkers);
		  System.out.println("files for every worker in spoolout created");
		  Job jobInstance = null;
		  try {
			  Class<?> jobClass = Class.forName(jobName);
			  jobInstance = (Job)jobClass.newInstance();
		  } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  Context contextInstance = new MapContextImpl(spoolOutFiles);
		  System.out.println("map context instance created");
			  
		  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
		  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
		  for(int i = 0; i<numThreads-1; i++){
			  service.submit(new MapThread(queue, jobInstance, contextInstance));
		  }
		  String inputDir = storageDir.concat(input);
		  System.out.println("[debug] input directory full path is"+inputDir);
		  File folder = new File(inputDir);
		  File[] files = folder.listFiles();
		  int length = files.length;
		  System.out.println("reading files from disks to memory");
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
		  try {
			service.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  System.out.println("all threads have finished hashing and writing to spool-out files.");
		  System.out.println("[keysRead] "+keysRead+" keys have been read by last map");
		  System.out.println("[keysWritten] "+ keysWritten+" keys have been written by last map");
		  System.out.println("now posting contents of files to /pushdata of other workers");
		  for(int i = 0; i< numWorkers; i++){
			  StringBuffer workerName = new StringBuffer("worker");
			  for(String s: jobParams.keySet()){
				  System.out.println(s+"\t"+jobParams.get(s)[0]);
			  }
			  workerName.append(i+1);
			  String IPAndPort = jobParams.get(workerName.toString())[0];
			  /*
			  if(i == numWorkers-1){
				  IPAndPort = IPAndPort+String.valueOf(i);
			  }
			  */
			  String[] values = IPAndPort.trim().split(":");
			  InetAddress ip = InetAddress.getByName(values[0]);
			  String hostname = ip.getHostName();
			  System.out.println("IP:port is "+IPAndPort+", hostname: "+ hostname);
			  HttpClient client = new HttpClient(ip, Integer.valueOf(values[1]),hostname);
			  client.setRequestMethod("POST");
			  try {
				client.setRequestURL("worker", "pushdata");
			  } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			  }
			  StringBuffer filePath = new StringBuffer(storageDir);
			  filePath.append("spool-out/").append(workerName);
			  System.out.println("file path of "+workerName+"is: "+filePath);
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
			  System.out.println("body message in pushdata request:");
			  System.out.println(body);
			  client.requestFlush();
			  BufferedReader br = client.getInputStreamReader();
			  System.out.println("[debug]first line response from /pushdata: "+ br.readLine());
			  client.closeClient();			  
		  }
		  System.out.println("-------map results posted to /pushdata------");
		  System.out.println("worker status changes to waiting");
		  status = "waiting";
		  try {
			reportStatus();
		  } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
		  System.out.println("status updated...waiting");
	  }
	  else if(pathInfo.equalsIgnoreCase("/worker/pushdata")){
		  System.out.println("receieves post request from peer workers");
		  System.out.println("start writing body messages into spool-in directory...");
		  BufferedReader contentReader = request.getReader();
		  File intermediate = new File(storageDir+"spool-in/intermediate");
		  PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(intermediate,true)));
		  String line;
		  System.out.println("-----content data-----");
		  contentReader.readLine();
		  while((line = contentReader.readLine())!=null){
			  System.out.println(line);
			  pw.println(line);
		  }
		  System.out.println("-----data in content have been written to spool-in file");
		  pw.close();
	  }
	  else if(pathInfo.equalsIgnoreCase("/worker/runreduce")){
		  System.out.println("worker starts doing reducing...");
		  keysRead = 0;
		  keysWritten = 0;
		  status = "reducing";
		  //String jobName = request.getParameter("job");
		  @SuppressWarnings("unchecked")
		  Map<String,String[]>jobParams = request.getParameterMap();
		  for(String s: jobParams.keySet()){
			  System.out.println(s+"\t"+jobParams.get(s)[0]);
			  if(s.contains("job")) jobName = jobParams.get(s)[0];
		  }
		  //WorkerServlet.jobName = jobName;
		  String output = request.getParameter("output");
		  String num = request.getParameter("numThreads");
		  System.out.println("[debug]"+ num);
		  int numThreads = Integer.valueOf(num);
		  //int numThreads = 3;
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
		  System.out.println("output directory is created.");
		  File outputData = makeFile(storageDir, output, "outputdata");
		  System.out.println("outputdata file is created.");
		  File spoolIn = new File(storageDir+"spool-in");
		  File intermediate = new File(storageDir+"spool-in/intermediate");
		  System.out.println("start sorting the intermediate file");
		  sortFile(spoolIn);
		  Context contextInstance = new ReduceContextImpl(outputData);
		  System.out.println("reduce context instance created");
		  
		  BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
		  ExecutorService service = Executors.newFixedThreadPool(Integer.valueOf(numThreads));
		  for(int i = 0; i<numThreads; i++){
			  service.submit(new ReduceThread(queue, jobInstance, contextInstance));
		  }
		  System.out.println("reading files from disks to memory");
		  BufferedReader br = new BufferedReader(new FileReader(intermediate));
		  try {
			  readFileToMemo(queue,br);
		  } catch (InterruptedException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  service.shutdown();
		  try {
			service.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  System.out.println("reducing finished");
		  status = "idle";
		  try {
			reportStatus();
		  } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
		  System.out.println("status updated...idle");
	  }
  }
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  /*
	  String masterComb = getServletConfig().getInitParameter("master");
	  System.out.println("-----worker debug section-----");
	  System.out.println("[debug]ip:port of this master is =>"+masterComb);
	  String[] strings = masterComb.split(":");
	  String masterIP = strings[0];
	  String masterPort = strings[1];
	  
	  String workerPort = getServletConfig().getInitParameter("port");
	  String storageDir = getServletConfig().getInitParameter("storagedir");
	  */
  }
}
  
