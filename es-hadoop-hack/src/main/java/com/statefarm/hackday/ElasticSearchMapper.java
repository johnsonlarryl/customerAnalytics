package com.statefarm.hackday;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElasticSearchMapper extends Mapper<Text, MapWritable, Text, MapWritable>  {
	@Override
	  public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value); // ES Hadoop ignores the key...need to write NullWritable
	  }
}
