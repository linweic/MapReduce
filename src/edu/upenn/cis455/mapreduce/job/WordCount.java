package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  // Your map function for WordCount goes here
	  String[] words = value.trim().split("\\s");
	  for(String word: words){
		  context.write(word, "1");
	  }

  }
  
  public void reduce(String key, String[] values, Context context)
  {
	  // Your reduce function for WordCount goes here
	  int result = 0;
	  for(String value: values){
		  result += Integer.parseInt(value);
	  }
	  context.write(key, String.valueOf(result));
  }
  
}
