package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class FileTask implements Runnable{
	private final BlockingQueue<String> queue;
	private final BufferedReader reader;
	public FileTask(BlockingQueue<String> queue, BufferedReader br){
		this.queue = queue;
		this.reader = br;
	}
	@Override
	public synchronized void run() {
		// TODO Auto-generated method stub
		//BufferedReader br = null;
		try {
			//br = new BufferedReader(reader);
			String line;
			while((line = reader.readLine())!=null){
				queue.put(line);
				System.out.println(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
