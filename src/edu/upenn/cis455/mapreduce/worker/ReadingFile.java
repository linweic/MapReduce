package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReadingFile {
	public static synchronized void readFileToMemo(BlockingQueue<String> queue, BufferedReader br) throws IOException, InterruptedException{
		String line;
		while((line = br.readLine())!=null){
			queue.put(line);
			System.out.println(Thread.currentThread().getName()+" [FileTask]"+line+" added to the queue");
		}
	}
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException{
		final int threadCount = 5;
		BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
		ExecutorService service = Executors.newFixedThreadPool(threadCount);
		
		for(int i = 0; i<5; i++){
			service.submit(new WorkerThread(queue,"edu.upenn.cis455.mapreduce.job.WordCount"));
		}
		File folder = new File("./testDir");
		File[] files = folder.listFiles();
		int length = files.length;

		for(int j = 0; j<length;j++){
			//System.out.println(files[j]);
			BufferedReader br = new BufferedReader(new FileReader(files[j]));
			//service.submit(new FileTask(queue,br)).get();
			readFileToMemo(queue, br);
		}
		
		
		service.shutdownNow();
		service.awaitTermination(365,TimeUnit.DAYS);
	}
}
