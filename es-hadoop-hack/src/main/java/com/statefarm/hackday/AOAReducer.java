package com.statefarm.hackday;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.AUTO;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.AUTO_AND_LIFE;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.AUTO_LIFE_AND_FIRE;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.FIRE;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.FIRE_AND_LIFE;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.LIFE;
import static com.statefarm.hackday.constant.CustomerProductFootPrint.UNIDENTIFIED_PRODCUCT;
import static com.statefarm.hackday.constant.CustomerTypes.LOYAL_CUSTOMER;
import static com.statefarm.hackday.constant.CustomerTypes.NEW_CUSTOMER;
import static com.statefarm.hackday.constant.CustomerTypes.NEW_EXISTING_CUSTOMER;
import static com.statefarm.hackday.constant.CustomerTypes.RECENT_CUSTOMER;
import static com.statefarm.hackday.constant.CustomerTypes.RETURNING_CUSTOMER;
import static com.statefarm.hackday.constant.CustomerTypes.UNIDENTIFIED_CUSTOMER;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.LocalDate;
import org.joda.time.Months;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.statefarm.hackday.constant.InputFields;
import com.statefarm.hackday.constant.OutputFields;

public class AOAReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

  private String DATE_FORMAT = "yyyy-MM-dd";
  private Format sdf = new SimpleDateFormat(DATE_FORMAT);
  private static final int LOYAL_CUSTOMER_MONTHS = 60;
  private static final int NEW_CUSTOMER_MONTHS = 6;
  private Logger logger = LoggerFactory.getLogger(AOAReducer.class);
	
  @Override
  public void reduce(Text key, Iterable<MapWritable> values, Context context)
      throws IOException, InterruptedException {

	  Text customerId = new Text();
	  int count = 0; 
	  boolean returningCustomer = false;
	  boolean activeCustomer = false;
	  
	  double totalMonthlyPremium = 0;
	  
	  LocalDate annivesaryDate = null;
	  LocalDate departureDate = null;
	  
	  LongWritable EMPTY_DATE = new LongWritable(0);
	  
	  List<Text> productFootPrint = new ArrayList<Text>();
	  
	  IntWritable lastContactWithCompany = new IntWritable();
	  
	  IntWritable zipCode = new IntWritable();
	  
	  for (MapWritable value : values) {
		  
		  logger.warn("Writing POLICY START DATE: " + value.get(InputFields.START_DATE));
		  
		  logger.warn("Object Type: " + value.get(InputFields.START_DATE).getClass());		  
		  
		  LocalDate policyStartDate = getLocalDate((LongWritable)value.get(InputFields.START_DATE));
		  		  
		  LongWritable tempEndDate = (LongWritable)value.get(InputFields.END_DATE);
				  
		  LocalDate policyEndDate = tempEndDate.equals(EMPTY_DATE) ? null : getLocalDate((LongWritable)value.get(InputFields.END_DATE));
		  
		  if (policyStartDate != null) {
			  if (annivesaryDate == null) {
				  annivesaryDate = new LocalDate(new Date());
			  }
			  
			  annivesaryDate = getSmallerDate(annivesaryDate, policyStartDate);
		  }
		  
		  if (policyEndDate != null) {
			  if (departureDate == null) {
				  departureDate = new LocalDate(new Date());
			  }
			  
			  departureDate = getLargerDate(departureDate, policyEndDate);
		  }
		  
		  // Get these values once as we know they should be consistent across all records
		  if (count == 0) {
			  returningCustomer = ((BooleanWritable) value.get(InputFields.RETURNING_CUSTOMER)).get();
			  activeCustomer = ((BooleanWritable) value.get(InputFields.ACTIVE_CUSTOMER)).get();
			  customerId.set((Text) value.get(InputFields.CUSTOMER_ID));
			  lastContactWithCompany = (IntWritable) value.get(InputFields.LAST_CONTACT_WITH_COMPANY);
			  zipCode = (IntWritable) value.get(InputFields.ZIPCODE);
		  }
		  
		  MapWritable reducerMap = new MapWritable();
		  reducerMap.put(OutputFields.CUSTOMER_ID, value.get(InputFields.CUSTOMER_ID));
		  
		  productFootPrint.add((Text)value.get(InputFields.POLICYTYPE));
		  
		  totalMonthlyPremium += ((DoubleWritable) value.get(InputFields.MONTHLY_PREMIUM)).get();
	  }
	  
	  // This is an active customer...it better be!s
	  if (departureDate == null) {
		  departureDate = new LocalDate(new Date());
	  }
	  
	  int loyaltyIndex = calculateLoyaltyIndex(annivesaryDate, departureDate);
	  
	  MapWritable reduceMap = new MapWritable();
	  reduceMap.put(OutputFields.CUSTOMER_ID, customerId);
	  reduceMap.put(OutputFields.SEGMENT, getCustomerSegment(returningCustomer, activeCustomer, loyaltyIndex));
	  reduceMap.put(OutputFields.TOTAL_MONTHLY_PREMIUM, new DoubleWritable(totalMonthlyPremium));
	  reduceMap.put(OutputFields.PRODUCT_FOOT_PRINT, getProductFootPrint(productFootPrint));
	  reduceMap.put(OutputFields.LAST_CONTACT_WITH_COMPANY, lastContactWithCompany);
	  reduceMap.put(OutputFields.START_DATE, new Text(sdf.format(annivesaryDate.toDate())));
	  reduceMap.put(OutputFields.LOCATION, getGeoCode(zipCode));
	  
	  context.write(customerId, reduceMap);
	  
  }
  
  /**
   * Utility function to return the geocode based on the zipcode
   * @param zipCode
   * @return
   */
  private MapWritable getGeoCode(IntWritable zipCode) {
	  MapWritable map = new MapWritable();
	  map.put(OutputFields.LATITUDE, new DoubleWritable(90.0));
	  map.put(OutputFields.LONGITUDE, new DoubleWritable(90.0));
	  
	  return map;
	  // Call GeoCode stuff here
  }
  
  private Text getProductFootPrint(List<Text> productTypes) {
	  boolean hasFire = false;
	  boolean hasLife = false;
	  boolean hasAuto = false;
	  
	  for (Text productType : productTypes) {
		  
		  if (productType.equals(AUTO)) {
			  hasAuto = true;
		  } else if (productType.equals(LIFE)) {
			  hasLife = true;
		  } else if (productType.equals(FIRE)) {
			  hasFire = true;
		  }
	  }
	  
	  if (hasAuto && hasLife && hasFire) {
		  return AUTO_LIFE_AND_FIRE;
	  } else if (hasAuto && hasLife && !hasFire) {
		  return AUTO_AND_LIFE;
	  } else if (hasAuto && !hasLife && !hasFire) {
		  return AUTO;
	  } else if (hasFire && hasLife) {
		  return FIRE_AND_LIFE;
	  } else if (hasFire && !hasAuto && !hasLife) {
		  return FIRE;
	  } else if (hasLife && !hasAuto && !hasFire) {
		  return LIFE;
	  } else {
		  return UNIDENTIFIED_PRODCUCT;
	  }
  }
  
  private LocalDate getLocalDate(LongWritable date) {
	  if (date != null) {
		  return new LocalDate(new Date(date.get()));
	  } else {
		  return null;
	  }
	   
  }
  
  private LocalDate getSmallerDate(LocalDate firstDate, LocalDate secondDate) {
	  if (firstDate.isBefore(secondDate)) {
		  return firstDate;
	  } else {
		  return secondDate;
	  }
  }
  
  private LocalDate getLargerDate(LocalDate firstDate, LocalDate secondDate) {
	  if (firstDate.isAfter(secondDate)) {
		  return firstDate;
	  } else {
		  return secondDate;
	  }
  }
  
  /**
   * Utility method to calculate the loyaltyIndex
   * @param customerAnnivesary
   * @param customerDepature
   * @return
   */
  
  private int calculateLoyaltyIndex(LocalDate customerAnnivesary, LocalDate customerDepature) {
	  if (customerAnnivesary != null && customerAnnivesary != null) {
		  return Months.monthsBetween(customerAnnivesary, customerDepature).getMonths();
	  } else {
		  return -1; // return no difference
	  }
  }
  
  private Text getCustomerSegment(boolean returningCustomer, 
		  						  boolean activeCustomer, 
		  						  int loyaltyIndex) {
	
	  if (loyaltyIndex > LOYAL_CUSTOMER_MONTHS &&
		  !returningCustomer && 
		  activeCustomer) {  // They are a Loyal Customer
		  return LOYAL_CUSTOMER;
	  } else if (loyaltyIndex < NEW_CUSTOMER_MONTHS && 
			  	 !returningCustomer && 
			  	 activeCustomer) { // They are a New Customer
		  return NEW_CUSTOMER;
	  } else if (loyaltyIndex < NEW_CUSTOMER_MONTHS &&
			  	 returningCustomer &&
			  	 activeCustomer) {  // They are a Returning Customer
		  return RECENT_CUSTOMER;
	  } else if ((loyaltyIndex > NEW_CUSTOMER_MONTHS && loyaltyIndex < LOYAL_CUSTOMER_MONTHS) &&
			  	  !returningCustomer &&
			  	  activeCustomer) {
		  return NEW_EXISTING_CUSTOMER;
	  } else if ((loyaltyIndex > NEW_CUSTOMER_MONTHS && loyaltyIndex < LOYAL_CUSTOMER_MONTHS) &&
			  	  returningCustomer &&
			  	  activeCustomer) {
		  return RETURNING_CUSTOMER;
	  } else if (loyaltyIndex < NEW_CUSTOMER_MONTHS &&
			  	 !returningCustomer &&
			  	 !activeCustomer) {
		  return RECENT_CUSTOMER;
	  } else if (loyaltyIndex > NEW_CUSTOMER_MONTHS &&
			  	 !returningCustomer &&
			  	 !activeCustomer) {
		  return RECENT_CUSTOMER;
	  } else {
		  return UNIDENTIFIED_CUSTOMER; // return an unidentified customer 
	  }
  }
}