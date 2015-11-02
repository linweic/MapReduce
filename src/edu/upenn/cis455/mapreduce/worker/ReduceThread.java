package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class ReduceThread implements Runnable{
	static final Logger logger = Logger.getLogger(ReduceThread.class);
	private final BlockingQueue<String> queue;
	private Job job;
	private Context context;
	
	public ReduceThread(BlockingQueue<String> queue, Job jobInstance, Context contextInstance){
		this.queue = queue;
		job = jobInstance;
		context = contextInstance;
	}
	public ReduceThread(BlockingQueue<String> queue){
		this.queue = queue;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//logger.debug(Thread.currentThread().getName()+" starts..");
		while(true){
			synchronized(queue){
				String line;
				String first = null;
				while(queue.peek()==null);
				first = queue.peek();
				//logger.debug("first read: "+first);
				String[] subString = first.split("\\t");
				String previousKey = subString[0];
				String currentKey = subString[0];
				List<String> valuesList = new ArrayList<String>();
				while(currentKey.equals(previousKey)){
					try {
						//logger.debug("----------");
						queue.take();
						logger.debug(Thread.currentThread().getName()+":"+subString[0]+"\t"+subString[1]);
						WorkerServlet.keysRead++;
						valuesList.add(subString[1]);
						previousKey = currentKey;
						line = queue.peek();
						if(line == null) break;
						subString = line.split("\\t");
						currentKey = subString[0];
						//job.map(strings[0], strings[1], context);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				String[] values = new String[valuesList.size()];
				valuesList.toArray(values);
				StringBuffer sb = new StringBuffer();
				for(String value: values){
					sb.append(value).append(" ");
				}
				logger.debug(previousKey+": "+sb);
				logger.debug("----------");
				job.reduce(previousKey,values,context);
			}
		}
	}

}
