package com.statefarm.hackday;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	

	private String AUTO = "AUTO";
	private String FIRE = "FIRE;";
	private String LIFE = "LIFE";
	
  private LongWritable result = new LongWritable();
  private MapWritable customerRecord = new MapWritable();
  private Text customerId = new Text();
  private Text segment = new Text();
  private DoubleWritable loyaltyIndex = new DoubleWritable();
  private DoubleWritable monthlyCost = new DoubleWritable();
  private IntWritable lastContactWithCompany  = new IntWritable();
  private MapWritable location = new MapWritable();
  private String DATE_FORMAT = "YYYY-MM-dd";
	
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

	  long sum = 0;
	  
	  for (LongWritable value : values) {
		  sum += value.get();
	  }
	  
	  /**
	  double monthlyPremium = Double.parseDouble(split[4]);
	  
	  if (policyType != null) {
		  if (policyType.equals(AUTO)) {
			  customerRecord.put(Fields., value)
		  } else if (policyType.equals(LIFE)) {
			  
		  } else if (policyType.equals(FIRE)) {
			  fireMonthlyCost
		  } else {
			  return; // process the next line...someone hosed the policy types.  Oh well.
		  }
	  
	  result.set(sum);
	  context.write(key, result);
	 */
  }
  
  /**
   * Utility method to get the start date as a Hadoop Text or String Object
   * @param date
   * @return
   */
  	private Text getStartDate(long date) {
		Format sdf = new SimpleDateFormat();
		Calendar cal = new GregorianCalendar();
		Date startDate = new Date(date);
		 
		return new Text(sdf.format(startDate));
  	}
}