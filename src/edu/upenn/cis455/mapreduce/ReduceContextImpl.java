package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class ReduceContextImpl implements Context{
	static final Logger logger = Logger.getLogger(ReduceContextImpl.class);
	private File outputFile;
	public ReduceContextImpl(File file){
		outputFile = file;
	}
	private void writeToFile(File file, StringBuffer sb) throws IOException{
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
		pw.println(sb.toString());
		pw.close();
	}
	@Override
	public void write(String key, String value) {
		// TODO Auto-generated method stub
		StringBuffer line = new StringBuffer(key);
		line.append("\t").append(value);
		try {
			writeToFile(outputFile, line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		WorkerServlet.keysWritten++;
	}

}
