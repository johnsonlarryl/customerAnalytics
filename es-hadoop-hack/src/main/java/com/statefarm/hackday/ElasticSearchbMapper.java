package com.statefarm.hackday;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElasticSearchbMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private Text customerId = new Text();
	private Text segment = new Text();
	private DoubleWritable loyaltyIndex = new DoubleWritable();
	private DoubleWritable fireMonthlyCost = new DoubleWritable();
	private DoubleWritable lifeMonthlyCost = new DoubleWritable();
	private DoubleWritable autoMonthlyCost = new DoubleWritable();
	private IntWritable lastContactWithCompany  = new IntWritable();
	private MapWritable location = new MapWritable();
	
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		// value is tab separated values: word, total occurrences
		String[] split = value.toString().split(",");
		
		if (split != null && split.length == 8) {
			 try { 
				 customerId.set(split[0]);
				 totalCount.set(Long.parseLong(split[1])));
				 
				 MapWritable doc = new MapWritable();
				 doc.put(new Text(""), customerId);
				 doc.put(new Text("count"), totalCount);
				 context.write(word, doc); // ES Hadoop ignores the key...need to write NullWritable
			 } catch (NumberFormatException e) {
				// cannot parse - ignore
				  
				  // we could take another action, such as incrementing a Mapreduce counter to 
				  // track how many liines it affects (see the getCounter() method on Context for
			 }
		}
	}
	
	private Text getStartDate(long startDate) {
		
	}
}
