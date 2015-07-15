package com.statefarm.hackday;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.statefarm.hackday.constant.InputFields;

public class MapReduceTest {
	private List<CustomerRecord> outputMapRecords;
	
	private String customerId1;
	private String customerId2;
	
	private String[] records;
	
	/*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
  
  /**
   * 	Sample Data Set
   * 	---------------
    	customerId			startDate			endDate	lastContact		monthlyPremium	customerType	zipCode	policyType
    	----------			---------			-------	-----------		--------------	
    	CID12663013925947,  1437627600000,  	0,  	3,  			62.0,  			New Customer,	38453,	AUTO
		CID12663074513005,  1434603600000,  	0,  	3,  			64.0,			New Customer,	38475,	AUTO

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
	  records = new String[]{"CID12663013925947,1437627600000,0,3,62.0,New Customer,38453,AUTO",
	  		  	 			 "CID12663074513005,1434603600000,0,3,64.0,New Customer,38475,AUTO"};
	  
	  
		 
	  CustomerRecord outputRecord1 = new CustomerRecord(customerId1, 1437627600000L, 0L, 3, 62.0, 38453, "AUTO");
	  CustomerRecord outputRecord2 = new CustomerRecord(customerId2, 1434603600000L,0L,3,64.0,38475,"AUTO");

	  
	  
	  outputMapRecords.add(outputRecord1);
	  outputMapRecords.add(outputRecord2);
	  

    /*
     * Set up the mapper test harness.
     */
    AOAMapper mapper = new AOAMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, MapWritable>();
    mapDriver.setMapper(mapper);
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
			  mapDriver.withOutput(new Text(outputMapRecords.get(record).getCustomerId()), map);
			  mapDriver.runTest();
		  }
	  } catch (IOException e) {
			fail("Mapper Unit Test Case failed: " + e.getMessage());
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
	  
	  public CustomerRecord(String customerId, 
			  				long startDate, 
			  				long endDate, 
			  				int lastContact, 
			  				double monthlyPremium, 
			  				int zipCode, 
			  				String policyType) {
		  this.customerId = customerId;
		  this.startDate = startDate;
		  this.endDate = endDate;
		  this.lastContact = lastContact;
		  this.monthlyPremium = monthlyPremium;
		  this.zipCode = zipCode;
		  this.policyType = policyType;
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
