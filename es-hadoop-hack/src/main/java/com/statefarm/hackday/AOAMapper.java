package com.statefarm.hackday;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.statefarm.hackday.constant.InputFields;

public class AOAMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			// value is csv: 
			// customerId, startDate, endDate, lastContact, monthlyPremium, customerType, zipCode, policyType
			String[] split = value.toString().split(",");
	  
			if (split != null && split.length == 8) {

				MapWritable map = convertToHadoopMap(split);
				
				context.write((Text)map.get(InputFields.CUSTOMER_ID), map);
			} else {
				// skip the line...someone screwed up with the data
			}	  					 
		} catch (Exception e) {
				  // cannot parse - ignore
				  
				  // we could take another action, such as incrementing a Mapreduce counter to 
				  // track how many liines it affects (see the getCounter() method on Context for details
		}
	} 
	
	private MapWritable convertToHadoopMap(String data[]) {
		MapWritable map = new MapWritable();
		
		map.put(InputFields.CUSTOMER_ID, new Text(data[0]));
		map.put(InputFields.START_DATE, new LongWritable(Long.parseLong(data[1])));
		map.put(InputFields.END_DATE, new LongWritable(Long.parseLong(data[2])));
		map.put(InputFields.LAST_CONTACT_WITH_COMPANY, new IntWritable(Integer.parseInt(data[3])));
		map.put(InputFields.MONTHLY_PREMIUM, new DoubleWritable(Double.parseDouble(data[4])));
		map.put(InputFields.ZIPCODE, new IntWritable(Integer.parseInt(data[6])));
		map.put(InputFields.POLICYTYPE, new Text(data[7]));
		
		return map;
		
	}
}
