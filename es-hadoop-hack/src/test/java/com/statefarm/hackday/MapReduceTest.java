package com.statefarm.hackday;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.statefarm.hackday.constant.CustomerProductFootPrint;
import com.statefarm.hackday.constant.CustomerTypes;
import com.statefarm.hackday.constant.InputFields;
import com.statefarm.hackday.constant.OutputFields;

/**
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.statefarm.hackday.constant.CustomerTypes;
import com.statefarm.hackday.constant.InputFields;
import com.statefarm.hackday.constant.OutputFields;
*/

public class MapReduceTest {
	private List<CustomerRecord> outputMapRecords;
	
	private String customerId1;
	private String customerId2;
	
	private String[] records;
	
	/*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  private MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
  private ReduceDriver<Text, MapWritable, Text, MapWritable> reduceDriver;

  private ArrayList<MapWritable> reducerShuffleValues1;
  
  private Text customerIdText1;
  
  private MapWritable mapOutput1;
  
  /**
   * 	Sample Data Set
   * 	---------------
    	customerId			startDate			endDate	lastContact		monthlyPremium	customerType	zipCode	policyType 	returningCustomer 	activeCustomer
    	----------			---------			-------	-----------		--------------	------------	-------	----------	-----------------	--------------
    	CID12663013925947,  1433140210658,  	0,  	3,  			62.0,  			New Customer,	38453,	AUTO		false				true
		CID12663074513005,  1433140210658,  	0,  	3,  			64.0,			New Customer,	38475,	AUTO		false				true

   */

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {
	  outputMapRecords = new ArrayList<CustomerRecord>();
	  
	  customerId1 = "CID12663013925947";
	  customerId2 = "CID12663074513005";
	  
	  // Java Fun Fact: According to the Java spec you cannot initial Array Instance members unless you 
	  // instantiate the Array Object with the new reserved word and the type of array you are initializing
	  records = new String[]{"CID12663013925947,1421560800000,0,3,62.0,New Customer,38453,AUTO,false,true",
	  		  	 			 "CID12663074513005,1421560800000,0,3,64.0,New Customer,38475,AUTO,false,true"};
	  
	  
		 
	  CustomerRecord outputRecord1 = new CustomerRecord(customerId1, 1421560800000L, 0L, 3, 62.0, 38453, "AUTO", false, true);
	  CustomerRecord outputRecord2 = new CustomerRecord(customerId2, 1421560800000L,0L,3,64.0,38475,"AUTO", false, true);

	  
	  
	  outputMapRecords.add(outputRecord1);
	  outputMapRecords.add(outputRecord2);
	  

    /*
     * Set up the mapper test harness.
     */
    AOAMapper mapper = new AOAMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, MapWritable>();
    mapDriver.setMapper(mapper);
    
    AOAReducer reducer = new AOAReducer();
    reduceDriver = new ReduceDriver<Text, MapWritable, Text, MapWritable>();
    reduceDriver.setReducer(reducer);
    
    reducerShuffleValues1 = new ArrayList<MapWritable>();
    
    
	  MapWritable map = new MapWritable();
	  
	  map.put(InputFields.CUSTOMER_ID, new Text(outputMapRecords.get(0).getCustomerId()));
	  map.put(InputFields.START_DATE, new LongWritable(outputMapRecords.get(0).getStartDate()));
	  map.put(InputFields.END_DATE, new LongWritable(outputMapRecords.get(0).getEndDate()));
	  map.put(InputFields.LAST_CONTACT_WITH_COMPANY, new IntWritable(outputMapRecords.get(0).getLastContact()));
	  map.put(InputFields.MONTHLY_PREMIUM, new DoubleWritable(outputMapRecords.get(0).getMonthlyPremium()));
	  map.put(InputFields.ZIPCODE, new IntWritable(outputMapRecords.get(0).getZipCode()));
	  map.put(InputFields.POLICYTYPE, new Text(outputMapRecords.get(0).getPolicyType()));
	  map.put(InputFields.RETURNING_CUSTOMER, new BooleanWritable(outputMapRecords.get(0).isReturningCustomer()));
	  map.put(InputFields.ACTIVE_CUSTOMER, new BooleanWritable(outputMapRecords.get(0).isActiveCustomer()));
	  reducerShuffleValues1.add(map);
    
	  customerIdText1 = new Text(customerId1);
	  
	  mapOutput1 = new MapWritable();
    
	  mapOutput1.put(OutputFields.CUSTOMER_ID, customerIdText1);
	  mapOutput1.put(OutputFields.SEGMENT, CustomerTypes.NEW_CUSTOMER);
	  mapOutput1.put(OutputFields.LOYALTY_INDEX, new IntWritable(1));
	  mapOutput1.put(OutputFields.AUTO_MOMTHLY_PREMIUM, new DoubleWritable(62.0));
	  mapOutput1.put(OutputFields.LIFE_MONTHLY_PREMIUM, new DoubleWritable(0));
	  mapOutput1.put(OutputFields.FIRE_MONTHLY_PREMIUM, new DoubleWritable(0));
	  mapOutput1.put(OutputFields.TOTAL_MONTHLY_PREMIUM, new DoubleWritable(62.0));
	  mapOutput1.put(OutputFields.LAST_CONTACT_WITH_COMPANY, new IntWritable(3));
	  
	  MapWritable location = new MapWritable();
	  map.put(OutputFields.LONGITUDE, new DoubleWritable(90.0));
	  map.put(OutputFields.LATITUDE, new DoubleWritable(90.0));
	  
	  mapOutput1.put(OutputFields.LOCATION, location);;
	  mapOutput1.put(OutputFields.START_DATE, new Text("2015-06-01"));
	  mapOutput1.put(OutputFields.PRODUCT_FOOT_PRINT, CustomerProductFootPrint.AUTO);
    
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {
	  try {
		  // Question: What is Long Writable as the first argument on the withInput method?
		  for (int record = 0; record < outputMapRecords.size(); record++) {
			  mapDriver.withInput(new LongWritable(), new Text(records[record]));
			  
			  MapWritable map = new MapWritable();
			  
			  map.put(InputFields.CUSTOMER_ID, new Text(outputMapRecords.get(record).getCustomerId()));
			  map.put(InputFields.START_DATE, new LongWritable(outputMapRecords.get(record).getStartDate()));
			  map.put(InputFields.END_DATE, new LongWritable(outputMapRecords.get(record).getEndDate()));
			  map.put(InputFields.LAST_CONTACT_WITH_COMPANY, new IntWritable(outputMapRecords.get(record).getLastContact()));
			  map.put(InputFields.MONTHLY_PREMIUM, new DoubleWritable(outputMapRecords.get(record).getMonthlyPremium()));
			  map.put(InputFields.ZIPCODE, new IntWritable(outputMapRecords.get(record).getZipCode()));
			  map.put(InputFields.POLICYTYPE, new Text(outputMapRecords.get(record).getPolicyType()));
			  map.put(InputFields.RETURNING_CUSTOMER, new BooleanWritable(outputMapRecords.get(record).isReturningCustomer()));
			  map.put(InputFields.ACTIVE_CUSTOMER, new BooleanWritable(outputMapRecords.get(record).isActiveCustomer()));
			  mapDriver.withOutput(new Text(outputMapRecords.get(record).getCustomerId()), map);
			  mapDriver.runTest();
		  }
	  } catch (IOException e) {
			fail("Mapper Unit Test Case failed: " + e.getMessage());
	  }
  }
  
  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {
	  try {
	          reduceDriver.withInput(customerIdText1, reducerShuffleValues1);
	          reduceDriver.withOutput(customerIdText1, mapOutput1);
	          reduceDriver.runTest();
	  } catch (Exception e) {
	          fail("Reducer Unit Test Case failed: " + e.getMessage());
	  }
  }
  
  
  class CustomerRecord {
	  private String customerId;
	  private long startDate;
	  private long endDate;
	  private int lastContact;
	  private double monthlyPremium; 
	  private int zipCode;
	  private String policyType;
	  private boolean returningCustomer;
	  private boolean activeCustomer;
	  
	public CustomerRecord(String customerId, 
			  				long startDate, 
			  				long endDate, 
			  				int lastContact, 
			  				double monthlyPremium, 
			  				int zipCode, 
			  				String policyType,
			  				boolean returningCustomer,
			  				boolean activeCustomer) {
		  this.customerId = customerId;
		  this.startDate = startDate;
		  this.endDate = endDate;
		  this.lastContact = lastContact;
		  this.monthlyPremium = monthlyPremium;
		  this.zipCode = zipCode;
		  this.policyType = policyType;
		  this.returningCustomer = returningCustomer;
		  this.activeCustomer = activeCustomer;
	}
	
	public boolean isReturningCustomer() {
		return returningCustomer;
	}

	public void setReturningCustomer(boolean returningCustomer) {
		this.returningCustomer = returningCustomer;
	}

	public boolean isActiveCustomer() {
		return activeCustomer;
	}

	public void setActiveCustomer(boolean activeCustomer) {
		this.activeCustomer = activeCustomer;
	}



	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public long getStartDate() {
		return startDate;
	}

	public void setStartDate(long startDate) {
		this.startDate = startDate;
	}

	public long getEndDate() {
		return endDate;
	}

	public void setEndDate(long endDate) {
		this.endDate = endDate;
	}

	public int getLastContact() {
		return lastContact;
	}

	public void setLastContact(int lastContact) {
		this.lastContact = lastContact;
	}

	public double getMonthlyPremium() {
		return monthlyPremium;
	}

	public void setMonthlyPremium(double monthlyPremium) {
		this.monthlyPremium = monthlyPremium;
	}

	public int getZipCode() {
		return zipCode;
	}

	public void setZipCode(int zipCode) {
		this.zipCode = zipCode;
	}

	public String getPolicyType() {
		return policyType;
	}

	public void setPolicyType(String policyType) {
		this.policyType = policyType;
	}
  }
}
