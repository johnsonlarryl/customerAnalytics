package com.statefarm.hackday.constant;

import org.apache.hadoop.io.Text;

public class CustomerTypes {
	/**
	 * Been with the company for 5 years
	 * Active customer
	 * Not returning customer
	 */
	public static Text LOYAL_CUSTOMER = new Text("Loyal Customer");
	
	/**
	 * Been with the company for less than 6 months
	 * Active customer
	 * Not returning customer
	 */
	public static Text NEW_CUSTOMER = new Text("New Customer");
	
	/**
	 * Been with the company for less than 6 months
	 * Active customer
	 * Returning customer
	 *   
	 */
	public static Text RETURNING_CUSTOMER = new Text("Returning Customer");
	
	/**
	 * Been with the company for more than 6 months, but less than 5 years
	 *  
	 */
	public static Text NEW_EXISTING_CUSTOMER = new Text("New Existing Customer");
	
	/**
	 * Been with the company for more than 6 months, but less than 5 years
	 */
	public static Text RETURNING_PREVIOUS_CUSTOMER = new Text("Returning Previous Customer");
	
	/**
	 * Left the company less than 6 months after policy was issued
	 */
	public static Text RECENT_CUSTOMER = new Text("Recent Customer");
	
	/**
	 * Left the company more than 6 months, after policy was issued
	 */
	public static Text PREVIOUS_CUSTOMER = new Text("Previous Customer");
	
	/**
	 * For some reason the logic was not executed properly to categorize the customer
	 */
	public static Text UNIDENTIFIED_CUSTOMER = new Text("Unidentified Customer");
	
	
}
