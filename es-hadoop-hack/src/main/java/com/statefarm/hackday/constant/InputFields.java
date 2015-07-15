package com.statefarm.hackday.constant;

import org.apache.hadoop.io.Text;

public interface InputFields {
	Text CUSTOMER_ID = new Text("customerId");
	Text START_DATE = new Text("startDate");
	Text END_DATE = new Text("endDate");
	Text LAST_CONTACT_WITH_COMPANY = new Text("lastContactWithCompany");
	Text MONTHLY_PREMIUM = new Text("monthlyPremium");
	Text ZIPCODE = new Text("zipCode");
	Text POLICYTYPE = new Text("policyType");
}
