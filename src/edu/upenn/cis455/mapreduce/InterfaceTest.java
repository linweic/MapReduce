package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class InterfaceTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Context context = new MapContextImpl();
		//context.write("a","b");
		File file = new File("./testDir/text3.txt");
		//FileWriter fw = new FileWriter(file, true);
		//BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
		pw.println("linwei\tchen");
		pw.close();
	}

}
