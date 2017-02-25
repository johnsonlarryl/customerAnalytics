package com.statefarm.es_hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElasticSearchbMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private Text word = new Text();
	private LongWritable totalCount = new LongWritable();
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		// value is tab separated values: word, total occurrences
		String[] split = value.toString().split("\t+");
		
		if (split != null && split.length == 2) {
			 try { 
				 word.set(split[0]);
				 totalCount.set(Long.parseLong(split[1]));
				 
				 MapWritable doc = new MapWritable();
				 doc.put(new Text("word"), word);
				 doc.put(new Text("count"), totalCount);
				 context.write(word, doc); // ES Hadoop ignores the key...need to write NullWritable
			 } catch (NumberFormatException e) {
				// cannot parse - ignore
				  
				  // we could take another action, such as incrementing a Mapreduce counter to 
				  // track how many liines it affects (see the getCounter() method on Context for
			 }
		}
	}
}
