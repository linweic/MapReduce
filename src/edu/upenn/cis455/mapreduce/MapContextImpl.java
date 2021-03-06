package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class MapContextImpl implements Context{
	static final Logger logger = Logger.getLogger(MapContextImpl.class);
	//private File spoolOut;
	private File[] files;
	/*
	public MapContextImpl(File name){
		spoolOut = name;
	}
	*/
	public MapContextImpl(File[] spoolOutFiles){
		files = spoolOutFiles;
	}
	public MapContextImpl(){}
	private void print(){
		System.out.println("Hello World");
	}
	private int hashKeys(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException{
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		md.reset();
		md.update(key.getBytes("UTF-8"));
		byte[] digest = md.digest();
		StringBuffer sb = new StringBuffer();
		for(byte b: digest){
			sb.append(String.format("%02X",b));
		}
		int numWorkers = files.length;
		System.out.println("[mapContextImpl] num of workers to hash "+ numWorkers);
		BigInteger tmp = BigInteger.ZERO.setBit(160);				
		BigInteger max = tmp.subtract(BigInteger.ONE);
		BigInteger interval = max.divide(BigInteger.valueOf(numWorkers));
		BigInteger hashcode = new BigInteger(digest);
		BigInteger index = hashcode.divide(interval);
		//System.out.println(max.toString());
		System.out.println(key+"'s hashcode is "+sb);
		int i = Integer.valueOf(index.toString());
		System.out.println("the key should be hashed to file["+i+"]");
		return i;
	}
	private synchronized void writeToFile(File file, StringBuffer sb) throws IOException{
		synchronized(file){
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
			pw.println(sb.toString());
			pw.close();
		}
	}
	@Override
	public void write(String key, String value) {
		// TODO Auto-generated method stub
		try {
			int index = hashKeys(key);
			StringBuffer line = new StringBuffer(key);
			line.append("\t").append(value);
			writeToFile(files[index], line);
			WorkerServlet.keysWritten++;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
