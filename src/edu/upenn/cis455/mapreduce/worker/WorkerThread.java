package edu.upenn.cis455.mapreduce.worker;

import java.util.concurrent.BlockingQueue;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WorkerThread implements Runnable{
	private final BlockingQueue<String> queue;
	private Job job;
	private Context context;
	
	public WorkerThread(BlockingQueue<String> queue, Job jobInstance, Context contextInstance){
		this.queue = queue;
		job = jobInstance;
		context = contextInstance;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		String line;
		while(true){
			try {
				line = queue.take();
				String[] strings = line.split("\\t");
				System.out.println("----------");
				System.out.println(Thread.currentThread().getName()+":"+strings[0]+"\t"+strings[1]);
				job.map(strings[0], strings[1], context);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				System.out.println(Thread.currentThread().getName()+": interrupted");
			}
		}
	}

}
