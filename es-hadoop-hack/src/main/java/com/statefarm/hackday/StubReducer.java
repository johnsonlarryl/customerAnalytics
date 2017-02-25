package com.statefarm.hackday;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  private LongWritable result = new LongWritable();
	
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

	  long sum = 0;
	  
	  for (LongWritable value : values) {
		  sum += value.get();
	  }
	  
	  result.set(sum);
	  context.write(key, result);
  }
}