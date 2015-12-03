package edu.upenn.cis455.mapreduce.job;

import java.util.List;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  // Your map function for WordCount goes here
	  String[] words = value.trim().split("\\s");
	  //List<String> words = Lemmatizer.lemmatize(value);
	  //System.out.println("---[WordCount job]---");
	  for(String word: words){
		  word = word.toLowerCase();
		  //System.out.println(word);
		  context.write(word, "1");
	  }
	  System.out.println("----------");
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
