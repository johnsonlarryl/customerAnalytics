package com.statefarm.hackday;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class AOADriver extends Configured implements Tool {

	
  public static void main(String[] args) throws Exception {
	  int rc = ToolRunner.run(new Configuration() , new AOADriver(), args); 
	  System.exit(rc);
	  
  }
  
  public int executeMapReduceJob(String args[]) throws Exception {
	String inputputFile = null;
  	String outputFile = null;
  	
    /*
     * Validate that two arguments were passed from the command line.
     */
  	System.out.println("Length is: " + args.length);
  	System.out.println("Argument is: " + args[0]);
    if (args.length != 2) {
      System.out.printf("Usage: AOODriver <input file> <output dir>\n");
      System.exit(-1);
    } else {
    	inputputFile = args[0];
    	outputFile = args[1];
    }
    
    Configuration conf = this.getConf();
    
    /*
     * Instantiate a Job object for your job's configuration. 
     *  
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    Job job = Job.getInstance(conf, "AOA Driver");
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(AOADriver.class);
    
   
    FileInputFormat.addInputPath(job, new Path(inputputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    job.setMapperClass(AOAMapper.class);
    job.setReducerClass(AOAReducer.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapWritable.class);
  

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    
    if (success) {
    	return executeElasticSearchJob(args);
    } else {
    	return statusCode(success);
    }
  }
  
  public static int statusCode(boolean success) {
  	return (success ? 0 : 1);
  }
  
  /**
   * Chained Map Job that will post the JSON data to the Elastic Search cluster
   * @param reduceDireOutputName
 * @throws Exception 
   */
  public int executeElasticSearchJob(String args[]) throws Exception {
	  String reduceDireOutputName = args[1];
	  
	  Configuration conf = this.getConf();
	  conf.set("es.nodes",  "192.168.1.223:9200");
	  conf.set("es.resource", "customersdata/customers"); // index/type
	    
	  Job job = Job.getInstance(conf, "Elastic Search Map Job");

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(MapWritable.class);
	  job.setMapOutputValueClass(MapWritable.class);
	  
	  job.setJarByClass(AOADriver.class);
	  
	  job.setMapperClass(ElasticSearchMapper.class);
	  job.setOutputFormatClass(EsOutputFormat.class);
	  
	  FileInputFormat.addInputPath(job, new Path(reduceDireOutputName + "/part-r-00000"));
	  
	  boolean success = job.waitForCompletion(true);
	   
	  return statusCode(success);
  }

	@Override
	public int run(String[] args) throws Exception {
		return executeMapReduceJob(args);
	}
}

