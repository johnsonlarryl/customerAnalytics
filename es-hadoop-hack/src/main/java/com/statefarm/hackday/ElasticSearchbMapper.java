package com.statefarm.hackday;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ElasticSearchbMapper extends Mapper<LongWritable, MapWritable, LongWritable, MapWritable>  {
	@Override
	  public void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value); // ES Hadoop ignores the key...need to write NullWritable
	  }
}
