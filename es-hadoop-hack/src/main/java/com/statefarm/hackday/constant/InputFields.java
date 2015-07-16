package com.statefarm.hackday.constant;

import org.apache.hadoop.io.Text;

public class InputFields {
	public static Text CUSTOMER_ID = new Text("customerId");
	public static Text START_DATE = new Text("startDate");
	public static Text END_DATE = new Text("endDate");
	public static Text LAST_CONTACT_WITH_COMPANY = new Text("lastContactWithCompany");
	public static Text MONTHLY_PREMIUM = new Text("monthlyPremium");
	public static Text ZIPCODE = new Text("zipCode");
	public static Text POLICYTYPE = new Text("policyType");
	public static Text RETURNING_CUSTOMER = new Text("returningCustomer");
	public static Text ACTIVE_CUSTOMER = new Text("activeCustomer");
}
