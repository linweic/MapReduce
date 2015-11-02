package edu.upenn.cis455.mapreduce.worker;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class MapThread implements Runnable{
	static final Logger logger = Logger.getLogger(MapThread.class);
	private final BlockingQueue<String> queue;
	private Job job;
	private Context context;
	
	public MapThread(BlockingQueue<String> queue, Job jobInstance, Context contextInstance){
		this.queue = queue;
		job = jobInstance;
		context = contextInstance;
	}
	public MapThread(BlockingQueue<String> queue){
		this.queue = queue;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		String line;
		while(true){
			try {
				line = queue.take();
				String[] strings = line.split("\\t");
				//logger.debug("----------");
				logger.debug(Thread.currentThread().getName()+":"+strings[0]+"\t"+strings[1]);
				WorkerServlet.keysRead++;
				job.map(strings[0], strings[1], context);
				/*
				MessageDigest md = MessageDigest.getInstance("SHA-1");
				md.reset();
				md.update(strings[0].getBytes("UTF-8"));
				byte[] digest = md.digest();
				StringBuffer sb = new StringBuffer();
				
				for(byte b: digest){
					sb.append(String.format("%02X",b));
				}
				BigInteger tmp = BigInteger.ZERO.setBit(160);				
				BigInteger max = tmp.subtract(BigInteger.ONE);
				//BigInteger oneOfFour = max.divide(BigInteger.valueOf(4));
				BigInteger hashcode = new BigInteger(digest);
				System.out.println(max.toString()+"\t"+ max.toString().length()+"\t"+max.signum());
				System.out.println(strings[0]+"'s hashcode is "+hashcode.toString()+"\t"+ hashcode.toString().length());
				BigInteger a = BigInteger.valueOf(4);
				BigInteger b = BigInteger.valueOf(9);
				BigInteger divide = b.divide(a);
				System.out.println(Integer.valueOf(divide.toString()));
				*/
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				logger.debug(Thread.currentThread().getName()+": interrupted unexpectedly.");
			}
		}
	}

}
