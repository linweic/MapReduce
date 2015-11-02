package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class InterfaceTest {
	private static void sortFile(File workDir){
		try {
			ProcessBuilder pb = new ProcessBuilder("sort","./test.txt");
			pb.directory(workDir);
			Process process;
			process = pb.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line;
			while((line = br.readLine())!= null){
				System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void catFile(File workDir){
		try {
			ProcessBuilder pb = new ProcessBuilder("ls","-al");
			pb.directory(workDir);
			Process process;
			process = pb.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line;
			while((line = br.readLine())!= null){
				System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Context context = new MapContextImpl();
		//context.write("a","b");
		/*
		File file = new File("./testDir/text3");
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
		pw.println("bruce\tchen");
		pw.close();
		*/
		File file = new File("./testDir");
		catFile(file);
		//System.out.println("----------");
		//sortFile(file);
		//System.out.println("----------");
		//catFile(file);
	}

}
