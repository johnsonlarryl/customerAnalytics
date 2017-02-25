package com.statefarm.es_hadoop;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text word = new Text();
	private LongWritable count = new LongWritable();
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  // value is tab separated values: word, year, occurrences, #books, #pages
	  // we project out (word, occurrences) so we can sum over all years
	  String[] split = value.toString().split("\t+");
	  
	  word.set(split[0]);
	  
	  if (split != null && split.length > 2) {
		  try { 
			  count.set(Long.parseLong(split[2]));
			  context.write(word, count);
		  } catch (NumberFormatException e) {
			  // cannot parse - ignore
			  
			  // we could take another action, such as incrementing a Mapreduce counter to 
			  // track how many liines it affects (see the getCounter() method on Context for details
		  }
	  }
	  
    

  }
}
